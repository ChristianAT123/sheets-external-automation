// index.js — External Google Sheets Automations
//  - Duplicates/blank highlighting on Column C across category tabs
//  - Status migrations (H/K) → Interested / Meeting Set (true move, safe deletes)
//  - MSN checkbox (F) → MSN Creators (true move, safe deletes)
//
// Local run example:
//   export SPREADSHEET_ID="<your_sheet_id>"
//   export GOOGLE_SERVICE_ACCOUNT_JSON="$(cat service-account.json)"
//   node index.js
//
// GitHub Actions uses repo secrets of the same names.

import { google } from "googleapis";

/* =========================
   CONFIG (tabs & columns)
   ========================= */

// Category/source tabs (from your existing workbook)
const SHEETS = [
  "GeneralCreators - Outreach", "CreatorsWithNoYoutube",
  "LongFormCreators - Outreach", "ASMR, Relaxation & Satisfying",
  "Toys & Kid-Focused Entertainment", // note your uploaded file uses the shortened name
  "Video Podcasts",
  "Personal Finance & Investing", "Health & Wellness",
  "Beauty & Fashion", "Gaming", "Education & How-To Content",
  "Business & Entrepreneurship", "Automotive", "Lifestyle & Vlogging",
  "Food & Cooking", "Travel", "Parenting & Family",
  "Home & DIY", "News & Commentary", "Music & Performance",
  "Movies & TV Commentary", "Currently In an MCN", "Science & Curiosity",
  "Luxury & High-End Lifestyle", "Real Estate & Investing",
  "Motivational & Self-Development"
];

// Include status destinations as part of scan set
const STATUS_SHEETS = SHEETS.concat(["Interested", "Meeting Set"]);

// Columns (1-based)
const STATUS_COLS = [8, 11];    // H and K
const ID_COL       = 20;        // T (unique ID)
const DATE_COL     = 1;         // A (manual timestamp)
const YT_COL       = 3;         // C (YouTube link/handle)

// Colors (hex)
const GREEN_HEX    = "#00ff00";
const BLANK_HEX    = "#c9daf8";
const DUP_HEX      = "#FF0000";

// MSN copier config
const MSN_DEST_SHEET       = "MSN Creators";
const MSN_ID_COL           = 20;      // T
const MSN_HEADER_ROW       = 1;
const MSN_CHECKBOX_COL     = 6;       // F
const MSN_PURPLE_HEX       = "#9900ff"; // not used when moving, kept for completeness
const MSN_SOURCE_WHITELIST = [];      // [] = all sources
const MSN_SKIP_SET         = new Set([MSN_DEST_SHEET, "Automation Log"]);

/* =========================
   Behavior switches
   ========================= */

// true = delete from source after confirmed copy; false = copy-only
const MIGRATE_STATUS_AS_MOVE = true;
const MIGRATE_MSN_AS_MOVE    = true;

// Safety checks
const REQUIRE_DEST_ID_BEFORE_DELETE      = true;  // only delete if ID landed in destination
const VERIFY_SOURCE_CHECKSUM_BEFORE_DELETE = true; // only delete if row content unchanged since copy

/* =========================
   Auth & client
   ========================= */

function getAuth() {
  const json = process.env.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!json) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON env var");
  const credentials = JSON.parse(json);
  const scopes = ["https://www.googleapis.com/auth/spreadsheets"];
  return new google.auth.JWT(
    credentials.client_email,
    null,
    credentials.private_key,
    scopes
  );
}
function sheetsClient(auth) {
  return google.sheets({ version: "v4", auth });
}

/* =========================
   Helpers
   ========================= */

function a1ColToIndex(col) {
  let n = 0;
  for (let i = 0; i < col.length; i++) n = n * 26 + (col.charCodeAt(i) - 64);
  return n;
}
function indexToA1Col(n) {
  let s = "";
  while (n > 0) {
    const r = (n - 1) % 26;
    s = String.fromCharCode(65 + r) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}
function sheetLastColLetter(sheet) {
  const cols = sheet.properties.gridProperties.columnCount || 26;
  return indexToA1Col(cols);
}
function hexToRgb01(hex) {
  const m = hex.replace("#", "");
  const r = parseInt(m.substring(0, 2), 16) / 255;
  const g = parseInt(m.substring(2, 4), 16) / 255;
  const b = parseInt(m.substring(4, 6), 16) / 255;
  return { red: r, green: g, blue: b };
}
function generateUniqueId() {
  return "id-" + (Date.now() + Math.floor(Math.random() * 1000)).toString(36);
}
function normalizeStr(str) {
  return str.toString().toLowerCase().replace(/[\._\s@]/g, "");
}
function extractHandle(url) {
  const s = url.toString().trim();
  const m = s.match(/@[^\/?&]+/);
  return m
    ? m[0].toLowerCase()
    : s
        .replace(/^(?:https?:\/\/)?(?:www\.)?youtube\.com\//i, "")
        .split(/[\/?&]/)[0]
        .toLowerCase();
}
function includesCase(text, needle) {
  return (text || "").toString().toLowerCase().includes(needle);
}
async function getSpreadsheetMeta(sheets, spreadsheetId) {
  const res = await sheets.spreadsheets.get({ spreadsheetId });
  return res.data;
}
async function getOrCreateSheet(sheets, spreadsheetId, title, metaCache) {
  let meta = metaCache ?? (await getSpreadsheetMeta(sheets, spreadsheetId));
  let sh = meta.sheets?.find((s) => s.properties?.title === title);
  if (sh) return { meta, sheet: sh };
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: { requests: [{ addSheet: { properties: { title } } }] },
  });
  meta = await getSpreadsheetMeta(sheets, spreadsheetId);
  sh = meta.sheets?.find((s) => s.properties?.title === title);
  return { meta, sheet: sh };
}
function simpleRowChecksum(rowArray) {
  const s = (rowArray || []).map(v => (v == null ? "" : String(v))).join("\u241F");
  let h = 5381;
  for (let i = 0; i < s.length; i++) h = ((h << 5) + h) + s.charCodeAt(i);
  return (h >>> 0).toString(36);
}

/* =========================
   Feature 1: Duplicates & blanks (Column C)
   ========================= */

async function highlightDuplicatesAndBlanksOnC(sheets, spreadsheetId) {
  const meta = await getSpreadsheetMeta(sheets, spreadsheetId);
  const allMap = {};        // handle → [{ sheetTitle, sheetId, row, date }]
  const blanksBySheet = {}; // sheetTitle → [rowNumbers]

  for (const title of SHEETS) {
    const s = meta.sheets?.find((x) => x.properties?.title === title);
    if (!s) continue;
    const lastRow = s.properties.gridProperties.rowCount || 2;
    if (lastRow < 2) continue;

    const rangeC = `${title}!C2:C${lastRow}`;
    const rangeA = `${title}!A2:A${lastRow}`;
    const [cVals, aVals] = await Promise.all([
      sheets.spreadsheets.values.get({ spreadsheetId, range: rangeC }),
      sheets.spreadsheets.values.get({ spreadsheetId, range: rangeA }),
    ]);
    const colC = (cVals.data.values || []).map((r) => (r?.[0] ?? ""));
    const colA = (aVals.data.values || []).map((r) => (r?.[0] ?? ""));

    colC.forEach((val, idx) => {
      const row = idx + 2;
      const v = (val || "").toString();
      if (!v) {
        (blanksBySheet[title] = blanksBySheet[title] || []).push(row);
      } else {
        const h = normalizeStr(extractHandle(v));
        (allMap[h] = allMap[h] || []).push({
          sheetTitle: title,
          sheetId: s.properties.sheetId,
          row,
          date: colA[idx] || "",
        });
      }
    });
  }

  const dupsBySheet = {};
  for (const [handle, list] of Object.entries(allMap)) {
    if (handle === "" || list.length <= 1) continue;
    list.sort((a, b) => {
      if (a.date < b.date) return -1;
      if (a.date > b.date) return 1;
      return a.row - b.row;
    });
    list.slice(1).forEach((rec) => {
      (dupsBySheet[rec.sheetTitle] = dupsBySheet[rec.sheetTitle] || []).push(rec.row);
    });
  }

  const requests = [];
  const blankColor = hexToRgb01(BLANK_HEX);
  const dupColor = hexToRgb01(DUP_HEX);

  for (const title of SHEETS) {
    const s = meta.sheets?.find((x) => x.properties?.title === title);
    if (!s) continue;
    const sheetId = s.properties.sheetId;

    (blanksBySheet[title] || []).forEach((r) => {
      requests.push({
        repeatCell: {
          range: {
            sheetId,
            startRowIndex: r - 1,
            endRowIndex: r,
            startColumnIndex: a1ColToIndex("C") - 1,
            endColumnIndex: a1ColToIndex("C"),
          },
          cell: { userEnteredFormat: { backgroundColor: blankColor } },
          fields: "userEnteredFormat.backgroundColor",
        },
      });
    });

    (dupsBySheet[title] || []).forEach((r) => {
      requests.push({
        repeatCell: {
          range: {
            sheetId,
            startRowIndex: r - 1,
            endRowIndex: r,
            startColumnIndex: a1ColToIndex("C") - 1,
            endColumnIndex: a1ColToIndex("C"),
          },
          cell: { userEnteredFormat: { backgroundColor: dupColor } },
          fields: "userEnteredFormat.backgroundColor",
        },
      });
    });
  }

  if (requests.length) {
    await sheets.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests } });
  }
}

/* =========================
   Feature 2: Status migrations (H/K) → Interested / Meeting Set
   ========================= */

async function runMigrations(sheets, spreadsheetId) {
  let meta = await getSpreadsheetMeta(sheets, spreadsheetId);

  // Ensure destination sheets exist
  await getOrCreateSheet(sheets, spreadsheetId, "Interested", meta);
  meta = await getSpreadsheetMeta(sheets, spreadsheetId);
  await getOrCreateSheet(sheets, spreadsheetId, "Meeting Set", meta);
  meta = await getSpreadsheetMeta(sheets, spreadsheetId);

  async function buildIdSet(title) {
    const s = meta.sheets?.find((x) => x.properties?.title === title);
    if (!s) return new Set();
    const lastRow = s.properties.gridProperties.rowCount || 2;
    if (lastRow < 2) return new Set();
    const range = `${title}!${indexToA1Col(ID_COL)}2:${indexToA1Col(ID_COL)}${lastRow}`;
    const res = await sheets.spreadsheets.values.get({ spreadsheetId, range });
    const vals = (res.data.values || []).map((r) => (r?.[0] ?? "")).map(String);
    return new Set(vals.filter(Boolean));
  }
  let interestedIds = await buildIdSet("Interested");
  let meetingIds    = await buildIdSet("Meeting Set");

  const copyRequests = [];
  const perSheetDeletePlan = new Map(); // title -> [{rowIdx1, checksum, dest, id}]

  for (const title of STATUS_SHEETS) {
    const s = meta.sheets?.find((x) => x.properties?.title === title);
    if (!s) continue;

    const sheetId = s.properties.sheetId;
    const lastRow = s.properties.gridProperties.rowCount || 2;
    const lastColLetter = sheetLastColLetter(s);
    if (lastRow < 2) continue;

    const rangeAll = `${title}!A2:${lastColLetter}${lastRow}`;
    const res = await sheets.spreadsheets.values.get({ spreadsheetId, range: rangeAll });
    const rows = res.data.values || [];

    const rangeId = `${title}!${indexToA1Col(ID_COL)}2:${indexToA1Col(ID_COL)}${lastRow}`;
    const idColRes = await sheets.spreadsheets.values.get({ spreadsheetId, range: rangeId });
    const idCol = (idColRes.data.values || []).map(r => String(r?.[0] ?? ""));

    async function getNextDestRow(destTitle, cached) {
      if (cached != null) return cached;
      const resA = await sheets.spreadsheets.values.get({
        spreadsheetId,
        range: `${destTitle}!A:A`,
      });
      const used = (resA.data.values || []).length;
      return used + 1;
    }
    let interestedNextRow = null;
    let meetingNextRow    = null;

    const idWrites = [];
    const toMaybeDelete = [];

    for (let i = 0; i < rows.length; i++) {
      const rowIdx1 = i + 2;
      const row = rows[i] || [];
      const colH = (row[STATUS_COLS[0] - 1] || "").toString();
      const colK = (row[STATUS_COLS[1] - 1] || "").toString();
      const statText = `${colH} ${colK}`.toLowerCase();

      let dest = null;
      if (statText.includes("meeting set")) dest = "Meeting Set";
      else if (statText.includes("interested") && !statText.includes("not interested"))
        dest = "Interested";
      if (!dest) continue;

      // Ensure ID
      let id = idCol[i];
      if (!id) {
        id = generateUniqueId();
        idCol[i] = id;
        idWrites.push([rowIdx1, id]);
      }

      // Skip if already present in dest
      if (dest === "Interested" && interestedIds.has(id)) continue;
      if (dest === "Meeting Set" && meetingIds.has(id)) continue;

      // Determine append row in destination
      if (dest === "Interested") {
        interestedNextRow = await getNextDestRow("Interested", interestedNextRow);
      } else {
        meetingNextRow = await getNextDestRow("Meeting Set", meetingNextRow);
      }
      const destRow = dest === "Interested" ? interestedNextRow++ : meetingNextRow++;

      // Copy values + formatting
      const destSheetId = meta.sheets?.find((x) => x.properties?.title === dest)?.properties?.sheetId;
      const colCount = s.properties.gridProperties.columnCount;

      copyRequests.push({
        copyPaste: {
          source: {
            sheetId,
            startRowIndex: rowIdx1 - 1,
            endRowIndex: rowIdx1,
            startColumnIndex: 0,
            endColumnIndex: colCount,
          },
          destination: {
            sheetId: destSheetId,
            startRowIndex: destRow - 1,
            endRowIndex: destRow,
            startColumnIndex: 0,
            endColumnIndex: colCount,
          },
          pasteType: "PASTE_NORMAL",
        },
      });

      if (MIGRATE_STATUS_AS_MOVE) {
        toMaybeDelete.push({
          rowIdx1,
          checksum: simpleRowChecksum(row),
          dest,
          id
        });
      }

      // Update caches
      if (dest === "Interested") interestedIds.add(id);
      else meetingIds.add(id);
    }

    // Write back any new IDs (sparse)
    if (idWrites.length) {
      const updates = idWrites.map(([r, v]) => ({
        range: `${title}!${indexToA1Col(ID_COL)}${r}:${indexToA1Col(ID_COL)}${r}`,
        values: [[v]],
      }));
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        requestBody: { valueInputOption: "RAW", data: updates },
      });
    }

    if (MIGRATE_STATUS_AS_MOVE && toMaybeDelete.length) {
      perSheetDeletePlan.set(title, toMaybeDelete);
    }
  }

  // Commit copies first
  if (copyRequests.length) {
    await sheets.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests: copyRequests } });
  }

  if (MIGRATE_STATUS_AS_MOVE) {
    // Reconfirm destination IDs after copy
    let meta2 = await getSpreadsheetMeta(sheets, spreadsheetId);
    async function buildIdSet2(title) {
      const s = meta2.sheets?.find((x) => x.properties?.title === title);
      if (!s) return new Set();
      const lastRow = s.properties.gridProperties.rowCount || 2;
      if (lastRow < 2) return new Set();
      const range = `${title}!${indexToA1Col(ID_COL)}2:${indexToA1Col(ID_COL)}${lastRow}`;
      const res = await sheets.spreadsheets.values.get({ spreadsheetId, range });
      const vals = (res.data.values || []).map((r) => (r?.[0] ?? "")).map(String);
      return new Set(vals.filter(Boolean));
    }
    const haveInterested = await buildIdSet2("Interested");
    const haveMeeting    = await buildIdSet2("Meeting Set");

    const deleteRequests = [];

    for (const [title, list] of perSheetDeletePlan.entries()) {
      const sheetMeta = meta.sheets?.find((x) => x.properties?.title === title);
      if (!sheetMeta) continue;

      // Optional checksum: re-read current source block
      let sourceRowsValues = null;
      if (VERIFY_SOURCE_CHECKSUM_BEFORE_DELETE) {
        const lastRow = sheetMeta.properties.gridProperties.rowCount || 2;
        const lastColLetter = sheetLastColLetter(sheetMeta);
        const rangeAll = `${title}!A2:${lastColLetter}${lastRow}`;
        const res = await sheets.spreadsheets.values.get({ spreadsheetId, range: rangeAll });
        sourceRowsValues = res.data.values || [];
      }

      // Decide which rows to delete
      const rowsToDelete = [];
      for (const item of list) {
        const { rowIdx1, checksum, dest, id } = item;
        if (REQUIRE_DEST_ID_BEFORE_DELETE) {
          const ok = dest === "Interested" ? haveInterested.has(id) : haveMeeting.has(id);
          if (!ok) continue; // don’t delete if the ID didn’t land
        }
        if (VERIFY_SOURCE_CHECKSUM_BEFORE_DELETE && sourceRowsValues) {
          const rowNow = sourceRowsValues[rowIdx1 - 2] || [];
          if (simpleRowChecksum(rowNow) !== checksum) continue; // changed in-source: skip delete
        }
        rowsToDelete.push(rowIdx1);
      }

      // Delete strictly bottom-up
      rowsToDelete.sort((a, b) => b - a).forEach((r) => {
        deleteRequests.push({
          deleteDimension: {
            range: {
              sheetId: sheetMeta.properties.sheetId,
              dimension: "ROWS",
              startIndex: r - 1,
              endIndex: r,
            },
          },
        });
      });
    }

    if (deleteRequests.length) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId,
        requestBody: { requests: deleteRequests },
      });
    }
  }
}

/* =========================
   Feature 3: MSN checkbox (F) → MSN Creators
   ========================= */

async function runMsnCheckboxCopy(sheets, spreadsheetId) {
  let meta = await getSpreadsheetMeta(sheets, spreadsheetId);
  const { sheet: destSheet } = await getOrCreateSheet(sheets, spreadsheetId, MSN_DEST_SHEET, meta);
  meta = await getSpreadsheetMeta(sheets, spreadsheetId);

  // Destination ID set
  const destLastRow = destSheet.properties.gridProperties.rowCount || 1;
  let destIdSet = new Set();
  if (destLastRow > MSN_HEADER_ROW) {
    const idRange = `${MSN_DEST_SHEET}!${indexToA1Col(MSN_ID_COL)}${MSN_HEADER_ROW + 1}:${indexToA1Col(MSN_ID_COL)}${destLastRow}`;
    const idRes = await sheets.spreadsheets.values.get({ spreadsheetId, range: idRange });
    const ids = (idRes.data.values || []).map(r => String(r?.[0] ?? "")).filter(Boolean);
    destIdSet = new Set(ids);
  }

  // Compute true bottom (ignores blanks and unchecked)
  const lcDest = Math.max(destSheet.properties.gridProperties.columnCount || 1, MSN_ID_COL);
  let nextDestRow = (await findTrueBottomRowAPI(
    sheets, spreadsheetId, MSN_DEST_SHEET, lcDest, MSN_HEADER_ROW, MSN_CHECKBOX_COL
  )) + 1;

  const copyRequests = [];
  const perSheetDeletePlan = new Map(); // title -> [{rowIdx1, checksum, id}]

  for (const s of meta.sheets || []) {
    const title = s.properties?.title || "";
    if (!title || MSN_SKIP_SET.has(title)) continue;
    if (MSN_SOURCE_WHITELIST.length && !MSN_SOURCE_WHITELIST.includes(title)) continue;

    const lr = s.properties.gridProperties.rowCount || 1;
    if (lr <= MSN_HEADER_ROW) continue;

    const lc = Math.max(s.properties.gridProperties.columnCount || 1, MSN_ID_COL);
    const rangeFlags = `${title}!${indexToA1Col(MSN_CHECKBOX_COL)}${MSN_HEADER_ROW + 1}:${indexToA1Col(MSN_CHECKBOX_COL)}${lr}`;
    const rangeAll   = `${title}!A${MSN_HEADER_ROW + 1}:${indexToA1Col(lc)}${lr}`;
    const [flagsRes, allRes] = await Promise.all([
      sheets.spreadsheets.values.get({ spreadsheetId, range: rangeFlags }),
      sheets.spreadsheets.values.get({ spreadsheetId, range: rangeAll }),
    ]);

    const flags = (flagsRes.data.values || []).map(r => r?.[0]);
    const rows  = allRes.data.values || [];

    // read current ID col for writes if needed
    const rangeId = `${title}!${indexToA1Col(MSN_ID_COL)}${MSN_HEADER_ROW + 1}:${indexToA1Col(MSN_ID_COL)}${lr}`;
    const idRes = await sheets.spreadsheets.values.get({ spreadsheetId, range: rangeId });
    const idCol = (idRes.data.values || []).map(r => String(r?.[0] ?? ""));

    const idWrites = [];
    const toMaybeDelete = [];

    for (let i = 0; i < rows.length; i++) {
      const rowIdx1 = MSN_HEADER_ROW + 1 + i;
      const isTrue = (flags[i] === true) || (String(flags[i]).toUpperCase() === "TRUE");
      if (!isTrue) continue;

      // Ensure ID
      let id = idCol[i];
      if (!id) {
        id = generateUniqueId();
        idCol[i] = id;
        idWrites.push([rowIdx1, id]);
      }
      if (destIdSet.has(id)) continue;

      // Paste values-only at nextDestRow
      copyRequests.push({
        pasteData: {
          data: (rows[i] || []).map(v => (v ?? "")).join("\t"),
          type: "PASTE_VALUES",
          delimiter: "\t",
          coordinate: {
            sheetId: destSheet.properties.sheetId,
            rowIndex: nextDestRow - 1,
            columnIndex: 0,
          },
        },
      });

      // Set F=TRUE at dest (to preserve “MSN selected” flag)
      copyRequests.push({
        updateCells: {
          rows: [{ values: [{ userEnteredValue: { boolValue: true } }] }],
          fields: "userEnteredValue",
          range: {
            sheetId: destSheet.properties.sheetId,
            startRowIndex: nextDestRow - 1,
            endRowIndex: nextDestRow,
            startColumnIndex: MSN_CHECKBOX_COL - 1,
            endColumnIndex: MSN_CHECKBOX_COL,
          }
        }
      });

      if (MIGRATE_MSN_AS_MOVE) {
        if (!perSheetDeletePlan.has(title)) perSheetDeletePlan.set(title, []);
        perSheetDeletePlan.get(title).push({
          rowIdx1,
          checksum: simpleRowChecksum(rows[i] || []),
          id
        });
      }

      destIdSet.add(id);
      nextDestRow++;
    }

    if (idWrites.length) {
      const updates = idWrites.map(([r, v]) => ({
        range: `${title}!${indexToA1Col(MSN_ID_COL)}${r}:${indexToA1Col(MSN_ID_COL)}${r}`,
        values: [[v]],
      }));
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId,
        requestBody: { valueInputOption: "RAW", data: updates },
      });
    }
  }

  // Commit copies
  if (copyRequests.length) {
    await sheets.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests: copyRequests } });
  }

  // If moving, verify IDs landed and delete bottom-up
  if (MIGRATE_MSN_AS_MOVE) {
    const idRange = `${MSN_DEST_SHEET}!${indexToA1Col(MSN_ID_COL)}${MSN_HEADER_ROW + 1}:${indexToA1Col(MSN_ID_COL)}`;
    const idRes = await sheets.spreadsheets.values.get({ spreadsheetId, range: idRange });
    const ids = (idRes.data.values || []).map(r => String(r?.[0] ?? "")).filter(Boolean);
    const haveId = new Set(ids);

    const deleteRequests = [];
    let meta2 = await getSpreadsheetMeta(sheets, spreadsheetId);

    for (const s of meta2.sheets || []) {
      const title = s.properties?.title || "";
      const plan = perSheetDeletePlan.get(title);
      if (!plan || !plan.length) continue;

      let sourceRowsValues = null;
      if (VERIFY_SOURCE_CHECKSUM_BEFORE_DELETE) {
        const lastRow = s.properties.gridProperties.rowCount || 2;
        const lastColLetter = sheetLastColLetter(s);
        const rangeAll = `${title}!A2:${lastColLetter}${lastRow}`;
        const res = await sheets.spreadsheets.values.get({ spreadsheetId, range: rangeAll });
        sourceRowsValues = res.data.values || [];
      }

      const rowsToDelete = [];
      for (const { rowIdx1, checksum, id } of plan) {
        if (REQUIRE_DEST_ID_BEFORE_DELETE && !haveId.has(id)) continue;
        if (VERIFY_SOURCE_CHECKSUM_BEFORE_DELETE && sourceRowsValues) {
          const rowNow = sourceRowsValues[rowIdx1 - 2] || [];
          if (simpleRowChecksum(rowNow) !== checksum) continue;
        }
        rowsToDelete.push(rowIdx1);
      }

      rowsToDelete.sort((a, b) => b - a).forEach((r) => {
        deleteRequests.push({
          deleteDimension: {
            range: {
              sheetId: s.properties.sheetId,
              dimension: "ROWS",
              startIndex: r - 1,
              endIndex: r,
            },
          },
        });
      });
    }

    if (deleteRequests.length) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId,
        requestBody: { requests: deleteRequests },
      });
    }
  }
}

/* =========================
   Support: true-bottom finder for MSN
   ========================= */

async function findTrueBottomRowAPI(sheets, spreadsheetId, title, lc, headerRow, checkboxCol) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${title}!A${headerRow + 1}:${indexToA1Col(lc)}`
  });
  const values = res.data.values || [];
  for (let i = values.length - 1; i >= 0; i--) {
    const row = values[i] || [];
    const rowIdx1 = headerRow + 1 + i;
    for (let c = 1; c <= lc; c++) {
      if (c === checkboxCol) {
        const v = row[c - 1];
        if (v === true) return rowIdx1; // checked counts as data
        continue; // unchecked FALSE ignored
      }
      const v = row[c - 1];
      if (v === null || v === "") continue;
      if (v === false) continue;
      if (typeof v === "string" && v.trim() === "") continue;
      return rowIdx1;
    }
  }
  return headerRow;
}

/* =========================
   Runner
   ========================= */

async function run() {
  const spreadsheetId = process.env.SPREADSHEET_ID;
  if (!spreadsheetId) throw new Error("Missing SPREADSHEET_ID env var");

  const auth = getAuth();
  const sheets = sheetsClient(auth);

  // 1) Duplicates + blanks on Column C
  await highlightDuplicatesAndBlanksOnC(sheets, spreadsheetId);

  // 2) Status migrations (Interested / Meeting Set)
  await runMigrations(sheets, spreadsheetId);

  // 3) MSN checkbox (F=TRUE → MSN Creators)
  await runMsnCheckboxCopy(sheets, spreadsheetId);

  console.log("Done: duplicates highlighted, rows migrated, MSN rows processed.");
}

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
