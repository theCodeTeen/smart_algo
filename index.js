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
  api_key: process?.env?.API_KEY, // Replace with your SmartAPI key
});

// Google Sheets API setup
const auth = new google.auth.GoogleAuth({
  keyFile: "/Users/dharmikbhadra/downloads/modern-rex-436213-q3-33411721d367.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const scripts = ["ADANIENT-EQ", "ADANIPORTS-EQ", "APOLLOHOSP-EQ", "ASIANPAINT-EQ", "AXISBANK-EQ", "BAJAJ-AUTO-EQ", "BAJFINANCE-EQ", "BAJAJFINSV-EQ", "BPCL-EQ", "BHARTIARTL-EQ", "BRITANNIA-EQ", "CIPLA-EQ", "COALINDIA-EQ", "DIVISLAB-EQ", "DRREDDY-EQ", "EICHERMOT-EQ", "GRASIM-EQ", "HCLTECH-EQ", "HDFCBANK-EQ", "HDFCLIFE-EQ", "HEROMOTOCO-EQ", "HINDALCO-EQ", "HINDUNILVR-EQ", "ICICIBANK-EQ", "ITC-EQ", "INDUSINDBK-EQ", "INFY-EQ","JSWSTEEL-EQ", "KOTAKBANK-EQ", "LTIM-EQ", "LT-EQ", "M&M-EQ", "MARUTI-EQ", "NTPC-EQ", "NESTLEIND-EQ", "ONGC-EQ", "POWERGRID-EQ", "RELIANCE-EQ", "SBILIFE-EQ", "SHRIRAMFIN-EQ", "SBIN-EQ", "SUNPHARMA-EQ", "TCS-EQ", "TATACONSUM-EQ", "TATAMOTORS-EQ", "TATASTEEL-EQ", "TECHM-EQ", "TITAN-EQ", "ULTRACEMCO-EQ","WIPRO-EQ"];
const tokens = [     "25",         "15083",          "157",           "236",          "5900",        "16669",          "317",          "16675",        "526",      "10604",          "547",         "694",      "20374",       "10940",        "881",       "910",          "1232",       "7229",        "1333",        "467",         "1348",          "1363",        "1394",          "4963",       "1660",     "5258",        "1594",    "11723",        "1922",      "17818",   "11483",  "2031",  "10999",     "11630",    "17963",       "2475",     "14977",         "2885",       "21808",      "4306",         "3045",     "3351",        "11536",   "3432",          "3456",          "3499",         "13538",   "3506",     "11532",        "3787"];
// Google Sheets ID and range
const SPREADSHEET_ID = "1QGkwnH834DvdPgjFmfSHB9PMmdpQBYol7em1bLvSkuI"; // Replace with your Google Sheet ID
const RANGE = "Sheet1!A1"; // Replace with the desired range in your sheet

// Function to fetch LTP (Last Traded Price) from SmartAPI
async function fetchLTP() {
  try {
    // Generate TOTP dynamically
    const totp = speakeasy.totp({
      secret: process?.env?.TOTP_SECRET, // Replace with your TOTP secret key
      encoding: "base32",
    });

    // Authenticate session
    const session = await api.generateSession(process?.env?.CLIENT_ID, process?.env?.MPIN, totp); // Replace with your SmartAPI credentials
    console.log(session);
    const jwtToken = session.data.jwtToken;

    // Define payload for market data request
    const payload = {
      			"mode": "LTP",
      			"exchangeTokens": {
      				"NSE": tokens
      			}
    }

    // Fetch LTP using market data API
    const response = await api.marketData(payload, jwtToken);
   
    const ltp = response?.data?.fetched;

    console.log("Fetched LTP:", ltp);
    return ltp;
  } catch (error) {
    console.error("Error fetching LTP:", error);
  }
}
function translateToRows(values){
  console.log(values);
  let finalRow = [values[0]]
  scripts?.map(script=>{
    console.log(script);
    const data = values[1]?.find(el=>{
      console.log(el);
      return el?.tradingSymbol==script
    });

    if(!data) finalRow.push(0+"");
    else finalRow.push(data?.ltp);
  })
  console.log(finalRow);
  return [finalRow];
}
// Function to append data to Google Sheets
async function appendData(authClient, values) {
  const sheets = google.sheets({ version: "v4", auth: authClient });

  console.log(translateToRows(values));
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
    console.log(response);
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



function main() {
  let startHour = 9; // Starting hour (9:30 AM)
  let endHour = 15;  // Ending hour (3:30 PM)

  // Schedule fetchAndUpdate to run at 9:30 AM IST
  fetchAndUpdate(); // Initial run at 9:30 AM

  // Schedule the task to run every hour until 3:30 PM IST
  let interval = setInterval(() => {
    const currentHour = new Date().getHours();
    const currentMinute = new Date().getMinutes();

    // Check if the time is within the allowed range (9:30 AM to 3:30 PM IST)
    if (currentHour >= startHour && currentHour <= endHour) {
      if (currentMinute === 30) { // Execute only if minutes are 30
        fetchAndUpdate();
      }
    } else {
      clearInterval(interval); // Stop the interval after 3:30 PM IST
    }
  }, 60000); // Check every minute
}

cron.schedule('30 9 * * *', main, {
  scheduled: true,
  timezone: 'Asia/Kolkata', // Set timezone to IST
});

// Run the first update immediately

