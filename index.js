// Required dependencies
const { google } = require("googleapis");
const { SmartAPI } = require("smartapi-javascript");
const speakeasy = require("speakeasy");
const fs = require("fs");
const cron = require('node-cron');
const dotenv = require("dotenv");
dotenv.config();

// Angel Broking SmartAPI credentials
const api = new SmartAPI({
  api_key: process.env.API_KEY, // Replace with your SmartAPI key
});

// Google Sheets API setup
const auth = new google.auth.GoogleAuth({
  keyFile: "/Users/dharmikbhadra/downloads/modern-rex-436213-q3-33411721d367.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

// Stock scripts & tokens
const scripts = [
  "ADANIENT-EQ","ADANIPORTS-EQ","APOLLOHOSP-EQ","ASIANPAINT-EQ","AXISBANK-EQ",
  "BAJAJ-AUTO-EQ","BAJFINANCE-EQ","BAJAJFINSV-EQ","BPCL-EQ","BHARTIARTL-EQ",
  "BRITANNIA-EQ","CIPLA-EQ","COALINDIA-EQ","DIVISLAB-EQ","DRREDDY-EQ","EICHERMOT-EQ",
  "GRASIM-EQ","HCLTECH-EQ","HDFCBANK-EQ","HDFCLIFE-EQ","HEROMOTOCO-EQ","HINDALCO-EQ",
  "HINDUNILVR-EQ","ICICIBANK-EQ","ITC-EQ","INDUSINDBK-EQ","INFY-EQ","JSWSTEEL-EQ",
  "KOTAKBANK-EQ","LTIM-EQ","LT-EQ","M&M-EQ","MARUTI-EQ","NTPC-EQ","NESTLEIND-EQ",
  "ONGC-EQ","POWERGRID-EQ","RELIANCE-EQ","SBILIFE-EQ","SHRIRAMFIN-EQ","SBIN-EQ",
  "SUNPHARMA-EQ","TCS-EQ","TATACONSUM-EQ","TATAMOTORS-EQ","TATASTEEL-EQ","TECHM-EQ",
  "TITAN-EQ","ULTRACEMCO-EQ","WIPRO-EQ"
];

const tokens = [
  "25","15083","157","236","5900","16669","317","16675","526","10604",
  "547","694","20374","10940","881","910","1232","7229","1333","467",
  "1348","1363","1394","4963","1660","5258","1594","11723","1922","17818",
  "11483","2031","10999","11630","17963","2475","14977","2885","21808","4306",
  "3045","3351","11536","3432","3456","3499","13538","3506","11532","3787"
];

// Google Sheets ID and range
const SPREADSHEET_ID = "1QGkwnH834DvdPgjFmfSHB9PMmdpQBYol7em1bLvSkuI";
const RANGE = "Sheet1!A1"; // Adjust if needed

// Function to fetch LTP (Last Traded Price) from SmartAPI
async function fetchLTP() {
  try {
    // Generate TOTP dynamically
    const totp = speakeasy.totp({
      secret: process.env.TOTP_SECRET, 
      encoding: "base32",
    });

    // Authenticate session
    const session = await api.generateSession(process.env.CLIENT_ID, process.env.MPIN, totp);
    console.log("Session:", session);
    const jwtToken = session.data.jwtToken;

    // Define payload for market data request
    const payload = {
      mode: "LTP",
      exchangeTokens: {
        NSE: tokens
      }
    };

    // Fetch LTP using market data API
    const response = await api.marketData(payload, jwtToken);
    const ltp = response?.data?.fetched || [];
    console.log("Fetched LTP:", ltp);
    return ltp;
  } catch (error) {
    console.error("Error fetching LTP:", error);
    return [];
  }
}

// Helper to translate the raw LTP array to row data
function translateToRows(values) {
  // values = [timestamp, ltpArray]
  const [timeStamp, ltpArray] = values;
  let finalRow = [timeStamp];

  // For each script in the same order as `scripts`,
  // find its LTP in ltpArray and push to finalRow
  scripts.forEach((script) => {
    const data = ltpArray.find(el => el?.tradingSymbol === script);
    finalRow.push(data ? data?.ltp : "0");
  });

  // Return as a 2D array (the way Sheets API expects)
  return [finalRow];
}

// Function to append data to Google Sheets
async function appendData(authClient, values) {
  const sheets = google.sheets({ version: "v4", auth: authClient });

  const request = {
    spreadsheetId: SPREADSHEET_ID,
    range: RANGE,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    resource: {
      values: translateToRows(values),
    },
  };

  try {
    const response = await sheets.spreadsheets.values.append(request);
    console.log(`Appended ${response.data.updates.updatedCells} cells.`);
  } catch (error) {
    console.error("Error appending data to Google Sheets:", error);
  }
}

// Main function to fetch LTP and update Google Sheets
async function fetchAndUpdate() {
  try {
    const ltp = await fetchLTP();
    const authClient = await auth.getClient();
    const timestamp = new Date().toISOString();

    await appendData(authClient, [timestamp, ltp]);
  } catch (error) {
    console.error("Error in fetchAndUpdate:", error);
  }
}
/**
 * -------------
 * CRON SCHEDULES
 * -------------
 * We will schedule fetchAndUpdate at:
 *   - 9:15 AM
 *   - Every hour from 10:00 AM through 3:00 PM (top of the hour)
 *   - 3:15 PM
 */

// 1) 9:15 AM
cron.schedule('15 9 * * *', fetchAndUpdate, {
  scheduled: true,
  timezone: 'Asia/Kolkata'
});

// 2) 10:00-14:00 on the hour => (10, 11, 12, 13, 14)
cron.schedule('0 10-14 * * *', fetchAndUpdate, {
  scheduled: true,
  timezone: 'Asia/Kolkata'
});

// 3) 3:00 PM (15:00)
cron.schedule('0 15 * * *', fetchAndUpdate, {
  scheduled: true,
  timezone: 'Asia/Kolkata'
});

// 4) 3:15 PM (15:15)
cron.schedule('15 15 * * *', fetchAndUpdate, {
  scheduled: true,
  timezone: 'Asia/Kolkata'
});

/**
 * (Optional) If you want to run something right when the server starts,
 * you can call fetchAndUpdate() here, but you said first data is 9:15 AM,
 * so we don't run it immediately.
 */

// fetchAndUpdate(); // Uncomment if you want an immediate run on startup
