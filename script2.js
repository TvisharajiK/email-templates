
const SOURCE_SPREADSHEET_ID = "1gr8YkGWIr1OXa11Ot8FHRPI_gub5Ya0srIOw7g0WBWQ";
const OUTPUT_SHEET_NAME = "Master_Aggregated";
const RETENTION_SHEET_NAME = "Retention_Matrix";

/**************************************************************
 * MASTER AGGREGATION (incremental append)
 **************************************************************/
function buildMasterSheet() {
  const destSS = SpreadsheetApp.getActiveSpreadsheet();
  const sourceSS = SpreadsheetApp.openById(SOURCE_SPREADSHEET_ID);

  const outputSheet =
    destSS.getSheetByName(OUTPUT_SHEET_NAME) ||
    destSS.insertSheet(OUTPUT_SHEET_NAME);

  const headers = [
    "Timestamp",
    "User ID",
    "Company ID",
    "Passed Suites",
    "Max Suites",
    "App ID",
    "Base URL",
    "Status",
    "Reason",
    "Email",
    "Source Sheet",
    "CohortWeek"
  ];

  if (outputSheet.getLastRow() === 0) {
    outputSheet.appendRow(headers);
  }

  const existingData =
    outputSheet.getLastRow() > 1
      ? outputSheet
          .getRange(2, 1, outputSheet.getLastRow() - 1, headers.length)
          .getValues()
      : [];

  const existingKeys = new Set(
    existingData.map(r => buildKey(r[0], r[1], r[5], r[10]))
  );

  const newRows = [];

  sourceSS.getSheets().forEach(sheet => {
    const data = sheet.getDataRange().getValues();
    if (data.length < 2) return;

    const header = data[0];
    const idx = name => header.indexOf(name);

    const col = {
      timestamp: idx("Timestamp"),
      userId: idx("User ID"),
      companyId: idx("Company ID"),
      passed: idx("Passed Suites"),
      max: idx("Max Suites"),
      appId: idx("App ID"),
      baseUrl: idx("Base URL"),
      status: idx("Status"),
      reason: idx("Reason"),
      email: idx("Email")
    };

    for (let i = 1; i < data.length; i++) {
      const r = data[i];
      const ts = col.timestamp > -1 ? r[col.timestamp] : "";
      if (!ts) continue;

      const userId = col.userId > -1 ? r[col.userId] : "";
      const appId = col.appId > -1 ? r[col.appId] : "";
      const key = buildKey(ts, userId, appId, sheet.getName());

      if (existingKeys.has(key)) continue;

      newRows.push([
        ts,
        userId,
        col.companyId > -1 ? r[col.companyId] : "",
        col.passed > -1 ? r[col.passed] : "",
        col.max > -1 ? r[col.max] : "",
        appId,
        col.baseUrl > -1 ? r[col.baseUrl] : "",
        col.status > -1 ? r[col.status] : "",
        col.reason > -1 ? r[col.reason] : "",
        col.email > -1 ? r[col.email] : "",
        sheet.getName(),
        getCohortWeek(ts)
      ]);

      existingKeys.add(key);
    }
  });

  if (newRows.length) {
    outputSheet
      .getRange(
        outputSheet.getLastRow() + 1,
        1,
        newRows.length,
        headers.length
      )
      .setValues(newRows);
  }
}

function buildKey(timestamp, userId, appId, sourceSheet) {
  return [
    timestamp instanceof Date ? timestamp.toISOString() : String(timestamp),
    userId || "",
    appId || "",
    sourceSheet
  ].join("|");
}

function getCohortWeek(timestamp) {
  const date =
    timestamp instanceof Date
      ? new Date(timestamp)
      : new Date(String(timestamp).substring(0, 10));

  const day = date.getDay();
  const diff = date.getDate() - (day === 0 ? 6 : day - 1);
  const monday = new Date(date.setDate(diff));

  return Utilities.formatDate(
    monday,
    SpreadsheetApp.getActive().getSpreadsheetTimeZone(),
    "yyyy-MM-dd"
  );
}

/**************************************************************
 * RETENTION MATRIX (rebuild from Master_Aggregated)
 **************************************************************/
function buildRetentionMatrix() {
  const ss = SpreadsheetApp.getActive();
  const source = ss.getSheetByName(OUTPUT_SHEET_NAME);
  if (!source) throw new Error("Master_Aggregated not found");

  const data = source.getDataRange().getValues();
  if (data.length < 2) return;

  const header = data[0];
  const idx = name => header.indexOf(name);

  const USER = idx("User ID");
  const DATE = idx("Timestamp");
  const EMAIL = idx("Email");
  const STATUS = idx("Status");
  const WEEK = idx("Source Sheet");

  const userMap = {};
  const weekAnchors = {};

  for (let i = 1; i < data.length; i++) {
    const r = data[i];
    if (!r[USER] || !r[WEEK]) continue;

    const key = r[USER] + "|" + (r[EMAIL] || "");
    if (!userMap[key]) userMap[key] = {};

    const weekObj = normalizeWeek(r[WEEK]);
    weekAnchors[weekObj.label] = weekObj.anchor;

   if (!userMap[key][weekObj.label]) {
  userMap[key][weekObj.label] = {
    passed: 0,
    failed: 0,
    date: r[DATE] instanceof Date
      ? Utilities.formatDate(
          r[DATE],
          ss.getSpreadsheetTimeZone(),
          "yyyy-MM-dd"
        )
      : ""
  };
}

    if (r[STATUS] === "SUCCESS") {
      userMap[key][weekObj.label].passed++;
    } else {
      userMap[key][weekObj.label].failed++;
    }
  }

  const weeks = Object.keys(weekAnchors).sort(
    (a, b) => weekAnchors[a] - weekAnchors[b]
  );

  const output = [];
  const headers = ["User ID", "Email", "Date"];

  for (let i = 0; i < weeks.length; i++) {
    headers.push(weeks[i] + " passed");
    headers.push(weeks[i] + " failed");
  }

  output.push(headers);

  for (const key in userMap) {
    const parts = key.split("|");
    let earliestDate = "";
    
    // Find the earliest date across all weeks for this user
    for (let i = 0; i < weeks.length; i++) {
      const w = weeks[i];
      const cell = userMap[key][w] || { passed: 0, failed: 0, date: "" };
      
      if (cell.date) {
        if (!earliestDate || cell.date < earliestDate) {
          earliestDate = cell.date;
        }
      }
    }

    const row = [parts[0], parts[1], earliestDate];

    for (let i = 0; i < weeks.length; i++) {
      const w = weeks[i];
      const cell = userMap[key][w] || { passed: 0, failed: 0, date: "" };

      row.push(cell.passed || 0);
      row.push(cell.failed || 0);
    }

    output.push(row);
  }

  const outSheet =
    ss.getSheetByName(RETENTION_SHEET_NAME) ||
    ss.insertSheet(RETENTION_SHEET_NAME);

  outSheet.clearContents();
  outSheet
    .getRange(1, 1, output.length, output[0].length)
    .setValues(output);
}

function normalizeWeek(label) {
  const m = label.match(/Week\s+(\d+)\s+([A-Za-z]+)\s+(\d{4})/);
  if (!m) throw new Error("Bad week label: " + label);

  const week = Number(m[1]);
  const monthName = m[2];
  const year = Number(m[3]);

  // Convert month name → month index (0-based)
  const monthIndex = new Date(monthName + " 1, 2000").getMonth();

  // Anchor date = first day of month + (week-1)*7
  const anchor = new Date(year, monthIndex, 1 + 7 * (week - 1));

  return {
    label: "Week " + week + " " + monthName.toLowerCase() + " " + year,
    anchor: anchor
  };
}




/**************************************************************
 * COHORT RETENTION (COUNT + PERCENT)
 **************************************************************/
function buildCohortRetentionMatrices() {
  const ss = SpreadsheetApp.getActive();
  const source = ss.getSheetByName("Master_Aggregated");
  if (!source) throw new Error("Master_Aggregated not found");

  const data = source.getDataRange().getValues();
  if (data.length < 2) return;

  const header = data[0];
  const idx = name => header.indexOf(name);

  const USER = idx("User ID");
  const WEEK = idx("Source Sheet");

  // ---------- 1. Collect first-seen cohort per user ----------
  const userFirstWeek = {}; // userId -> weekIndex
  const weekOrder = [];     // ordered list of weeks
  const weekIndex = {};     // weekLabel -> index

  function getWeekIndex(label) {
    if (weekIndex[label] === undefined) {
      weekIndex[label] = weekOrder.length;
      weekOrder.push(label);
    }
    return weekIndex[label];
  }

  for (let i = 1; i < data.length; i++) {
    const r = data[i];
    if (!r[USER] || !r[WEEK]) continue;

    const wIdx = getWeekIndex(r[WEEK]);
    const user = r[USER];

    if (userFirstWeek[user] === undefined) {
      userFirstWeek[user] = wIdx;
    }
  }

  // ---------- 2. Build cohort presence matrix ----------
  const cohorts = {}; // cohortWeekIdx -> { W0, W1, W2... }

  for (let i = 1; i < data.length; i++) {
    const r = data[i];
    if (!r[USER] || !r[WEEK]) continue;

    const user = r[USER];
    const currentWeekIdx = weekIndex[r[WEEK]];
    const cohortWeekIdx = userFirstWeek[user];

    const offset = currentWeekIdx - cohortWeekIdx;
    if (offset < 0) continue;

    if (!cohorts[cohortWeekIdx]) {
      cohorts[cohortWeekIdx] = {};
    }

    const key = "W" + offset;
    cohorts[cohortWeekIdx][key] = (cohorts[cohortWeekIdx][key] || new Set());
    cohorts[cohortWeekIdx][key].add(user);
  }

  // ---------- 3. Build COUNT matrix ----------
  const maxOffset = Math.max(
    ...Object.values(cohorts).flatMap(c =>
      Object.keys(c).map(k => Number(k.slice(1)))
    )
  );

  const countOutput = [];
  const headers = ["cohort_label"];
  for (let i = 0; i <= maxOffset; i++) headers.push("W" + i);
  countOutput.push(headers);

  const percentOutput = [headers.slice()];

  weekOrder.forEach((weekLabel, cohortIdx) => {
    if (!cohorts[cohortIdx]) return;

    const rowCount = [weekLabel];
    const rowPercent = [weekLabel];

    const w0 = cohorts[cohortIdx]["W0"]
      ? cohorts[cohortIdx]["W0"].size
      : 0;

    for (let i = 0; i <= maxOffset; i++) {
      const key = "W" + i;
      const val = cohorts[cohortIdx][key]
        ? cohorts[cohortIdx][key].size
        : 0;

      rowCount.push(val);
      rowPercent.push(w0 ? (val / w0) : 0);
    }

    countOutput.push(rowCount);
    percentOutput.push(rowPercent);
  });

  // ---------- 4. Write sheets ----------
  writeSheet(ss, "Retention_Count", countOutput);
  writeSheet(ss, "Retention_Percent", percentOutput, true);
}

/**************************************************************
 * Helpers
 **************************************************************/
function writeSheet(ss, name, values, formatPercent) {
  const sheet =
    ss.getSheetByName(name) || ss.insertSheet(name);

  sheet.clearContents();
  sheet
    .getRange(1, 1, values.length, values[0].length)
    .setValues(values);

  if (formatPercent) {
    sheet
      .getRange(2, 2, values.length - 1, values[0].length - 1)
      .setNumberFormat("0.0%");
  }
}

/**************************************************************
 * DAILY PIPELINE (recommended trigger target)
 **************************************************************/
function dailyPipeline() {
  buildMasterSheet();
  buildRetentionMatrix();
  buildCohortRetentionMatrices();
}
