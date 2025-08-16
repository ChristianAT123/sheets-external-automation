// index.js
// Run with: node index.js
// ENV required:
//   SPREADSHEET_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
//   GOOGLE_SERVICE_ACCOUNT_JSON='{"client_email":"...","private_key":"-----BEGIN PRIVATE KEY-----\n..."}'

/* =======================
   Imports & Auth
   ======================= */
import { google } from "googleapis";

const {
  SPREADSHEET_ID,
  GOOGLE_SERVICE_ACCOUNT_JSON,
} = process.env;

if (!SPREADSHEET_ID) throw new Error("Missing SPREADSHEET_ID env var");
if (!GOOGLE_SERVICE_ACCOUNT_JSON) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON env var");

const SA = JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON);
const auth = new google.auth.JWT(
  SA.client_email,
  null,
  SA.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth });

/* =======================
   Utilities
   ======================= */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

// Generic exponential backoff for 429/5xx & transient errors
async function withBackoff(fn, label = "request", maxAttempts = 6) {
  let attempt = 0;
  // jittered exponential backoff (cap 30s)
  while (true) {
    try {
      return await fn();
    } catch (e) {
      const code = e?.code || e?.response?.status;
      const retryable =
        code === 429 ||
        code === 503 ||
        code === 500 ||
        e?.message?.includes("rateLimitExceeded") ||
        e?.message?.includes("userRateLimitExceeded") ||
        e?.message?.includes("ETIMEDOUT") ||
        e?.message?.includes("ECONNRESET");

      if (!retryable || attempt >= maxAttempts - 1) {
        console.error(`[${label}] giving up after ${attempt + 1} attempt(s)`, e?.message || e);
        throw e;
      }

      const delay =
        Math.min(30000, 500 * 2 ** attempt) + Math.floor(Math.random() * 300);
      console.warn(
        `[${label}] retry ${attempt + 1}/${maxAttempts} in ${delay}ms (code:${code})`
      );
      await sleep(delay);
      attempt++;
    }
  }
}

/* =======================
   Sheets helpers
   ======================= */

// Batch GET many ranges at once
async function batchRead(spreadsheetId, ranges) {
  const res = await withBackoff(
    () =>
      sheets.spreadsheets.values.batchGet({
        spreadsheetId,
        ranges,
        valueRenderOption: "UNFORMATTED_VALUE",
        dateTimeRenderOption: "SERIAL_NUMBER",
        fields: "valueRanges(range,values)",
      }),
    "batchGet"
  );
  return res.data.valueRanges || [];
}

// Single wide read (prefer this when you can)
async function wideRead(spreadsheetId, a1Range) {
  const res = await withBackoff(
    () =>
      sheets.spreadsheets.values.get({
        spreadsheetId,
        range: a1Range,
        fields: "values",
      }),
    "values.get"
  );
  return res.data.values || [];
}

// Batch WRITE values (minimal calls)
async function batchWrite(spreadsheetId, data) {
  // data: [{ range: "Sheet1!A2:C5", values: [[...],[...]] }, ...]
  if (!data.length) return;

  await withBackoff(
    () =>
      sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        requestBody: {
          valueInputOption: "RAW",
          data,
        },
      }),
    "batchUpdate(values)"
  );
}

/* =======================
   Your workload config
   ======================= */

// Use your category sheets here (from your previous messages)
const CATEGORY_SHEETS = [
  "GeneralCreators - Outreach", "CreatorsWithNoYoutube",
  "LongFormCreators - Outreach", "ASMR, Relaxation & Satisfying",
  "Toys & Kid-Focused Entertainment", "Video Podcasts", "Technology & Gadgets",
  "Personal Finance & Investing", "Health & Wellness", "Beauty & Fashion",
  "Gaming", "Education & How-To Content", "Business & Entrepreneurship",
  "Automotive", "Lifestyle & Vlogging", "Food & Cooking", "Travel",
  "Parenting & Family", "Home & DIY", "News & Commentary",
  "Music & Performance", "Movies & TV Commentary", "Currently In an MCN",
  "Science & Curiosity", "Luxury & High-End Lifestyle",
  "Real Estate & Investing", "Motivational & Self-Development",
  "Interested", "Meeting Set"
];

// Default range window you usually read; adjust once and it affects all
const DEFAULT_RANGE = "A2:Z2209";

/* =======================
   Example business logic
   ======================= */

// Read all category sheets in batched chunks
async function readAllCategorySheets() {
  const allRanges = CATEGORY_SHEETS.map(
    (name) => `'${name}'!${DEFAULT_RANGE}`
  );

  const results = {};
  for (const group of chunk(allRanges, 10)) {
    const valueRanges = await batchRead(SPREADSHEET_ID, group);
    for (const vr of valueRanges) {
      const match = vr.range.match(/^'?(.+?)'?!/); // extract sheet name
      const sheetName = match ? match[1] : vr.range;
      results[sheetName] = vr.values || [];
    }
    // small pacing between batches
    await sleep(200);
  }
  return results;
}

// EXAMPLE: a migration that derives something and writes back to col T (just an example)
function computeUpdatesFromValues(valuesBySheet) {
  const updates = [];

  for (const [sheetName, rows] of Object.entries(valuesBySheet)) {
    if (!rows?.length) continue;

    // Example: write a status note in column T for each non-empty row (T is column 20)
    const writeBlock = [];
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      if (row && row.length && row.some((cell) => cell !== "" && cell !== null && cell !== undefined)) {
        writeBlock.push(["processed"]); // whatever your automation needs to write
      } else {
        writeBlock.push([""]); // keep empty to preserve height
      }
    }

    // Push a compact write for that sheet
    const range = `'${sheetName}'!T2:T${rows.length + 1}`;
    updates.push({ range, values: writeBlock });
  }

  return updates;
}

/* =======================
   Migrations & Runner
   ======================= */

export async function runMigrations() {
  // READ (batched)
  const valuesBySheet = await readAllCategorySheets();

  // YOUR LOGIC → compute write payloads
  const writes = computeUpdatesFromValues(valuesBySheet);

  // WRITE (batched)
  for (const group of chunk(writes, 20)) {
    await batchWrite(SPREADSHEET_ID, group);
    // Pace writes a bit to be nice to the API
    await sleep(150);
  }
}

export async function run() {
  console.log("Starting automations…");
  try {
    await runMigrations();
    console.log("Done ✅");
  } catch (e) {
    console.error("Automation failed ❌", e?.message || e);
    process.exitCode = 1;
  }
}

// Execute if called directly (GitHub Action runs `node index.js`)
if (import.meta.url === `file://${process.argv[1]}`) {
  run();
}
