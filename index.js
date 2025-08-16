import { google } from "googleapis";

/** ========= CONFIG (edit if needed) ========= */
const MASTER_SHEET = "Master";       // source tab
const TARGET_SHEET = "MSN Creators"; // destination tab
const CHECKBOX_COL_INDEX = 8;        // H (1-based)
const DUPLICATE_COL_INDEX = 3;       // C (1-based)
const READ_RANGE = "A1:Z";           // headers row 1; data starts row 2
/** ========================================= */

const { SPREADSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON } = process.env;
if (!SPREADSHEET_ID) throw new Error("Missing SPREADSHEET_ID");
if (!GOOGLE_SERVICE_ACCOUNT_JSON) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON");

const SA = JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON);
const auth = new google.auth.JWT(
  SA.client_email,
  null,
  SA.private_key,
  ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
);
const sheets = google.sheets({ version: "v4", auth });

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
async function withBackoff(fn, label = "req", max = 6) {
  for (let a = 0; a < max; a++) {
    try { return await fn(); }
    catch (e) {
      const code = e?.code || e?.response?.status;
      const retryable = [429, 500, 503].includes(code) ||
        /rateLimitExceeded|userRateLimitExceeded|ECONNRESET|ETIMEDOUT/.test(e?.message || "");
      if (!retryable || a === max - 1) throw e;
      const delay = Math.min(30000, 500 * 2 ** a) + Math.floor(Math.random() * 300);
      console.warn(`[${label}] retrying in ${delay}ms (code ${code})`);
      await sleep(delay);
    }
  }
}

async function ensureSheetExists(title) {
  const meta = await withBackoff(
    () => sheets.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID, fields: "sheets.properties" }),
    "spreadsheets.get"
  );
  const found = meta.data.sheets.find(s => s.properties.title === title);
  if (found) return found.properties.sheetId;

  const add = await withBackoff(
    () => sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests: [{ addSheet: { properties: { title } } }] }
    }),
    "addSheet"
  );
  return add.data.replies[0].addSheet.properties.sheetId;
}

async function getValues(sheet, a1 = READ_RANGE) {
  const res = await withBackoff(
    () => sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `'${sheet}'!${a1}`,
      fields: "values"
    }),
    "values.get"
  );
  return res.data.values || [];
}

async function clearRange(sheet, a1) {
  await withBackoff(
    () => sheets.spreadsheets.values.clear({
      spreadsheetId: SPREADSHEET_ID,
      range: `'${sheet}'!${a1}`
    }),
    "values.clear"
  );
}

async function batchWrite(data) {
  if (!data.length) return;
  await withBackoff(
    () => sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { valueInputOption: "RAW", data }
    }),
    "values.batchUpdate"
  );
}

function columnToLetter(col) {
  let s = "";
  while (col > 0) { const m = (col - 1) % 26; s = String.fromCharCode(65 + m) + s; col = (col - 1 - m) / 26; }
  return s;
}

async function setDuplicateRule(sheetId, colIndex) {
  const colLetter = columnToLetter(colIndex);
  const formula = `=COUNTIF($${colLetter}:$${colLetter}, ${colLetter}2)>1`;

  try {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: { requests: [{ updateConditionalFormatRule: { sheetId, index: 0, rule: {}, fields: "*" } }] }
    });
  } catch {}

  await withBackoff(
    () => sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [{
          addConditionalFormatRule: {
            index: 0,
            rule: {
              ranges: [{ sheetId, startRowIndex: 1, startColumnIndex: colIndex - 1, endColumnIndex: colIndex }],
              booleanRule: {
                condition: { type: "CUSTOM_FORMULA", values: [{ userEnteredValue: formula }] },
                format: { backgroundColor: { red: 0.973, green: 0.843, blue: 0.855 } }
              }
            }
          }
        }]
      }
    }),
    "addCFRule"
  );
}

async function materialize() {
  const targetSheetId = await ensureSheetExists(TARGET_SHEET);

  const values = await getValues(MASTER_SHEET, READ_RANGE);
  if (!values.length) { console.log("No data."); return; }

  const [headers, ...rows] = values;
  const flagIdx = CHECKBOX_COL_INDEX - 1;

  const selected = rows.filter(r => {
    const v = (r[flagIdx] ?? "").toString().trim().toLowerCase();
    return v === "true" || v === "1" || v === "yes" || v === "y";
  });

  await clearRange(TARGET_SHEET, "A:Z");
  const data = [{ range: `'${TARGET_SHEET}'!A1`, values: [headers] }];
  if (selected.length) data.push({ range: `'${TARGET_SHEET}'!A2`, values: selected });
  await batchWrite(data);

  await setDuplicateRule(targetSheetId, DUPLICATE_COL_INDEX);

  console.log(`Materialized ${selected.length} rows → "${TARGET_SHEET}" from "${MASTER_SHEET}".`);
}

(async function run() {
  try { await materialize(); }
  catch (e) { console.error("Run failed:", e?.message || e); process.exitCode = 1; }
})();
