const { google } = require("googleapis");
const { SmartAPI } = require("smartapi-javascript");
const speakeasy = require("speakeasy");
const cron = require("node-cron");
const dotenv = require("dotenv");
dotenv.config();

const api = new SmartAPI({
  api_key: process.env.API_KEY,
});

const auth = new google.auth.GoogleAuth({
  keyFile: process.env.GOOGLE_SERVICE_ACCOUNT_PATH,
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const scripts = [
  "ADANIENT-EQ", "ADANIPORTS-EQ", "APOLLOHOSP-EQ", "ASIANPAINT-EQ", "AXISBANK-EQ",
  "BAJAJ-AUTO-EQ", "BAJFINANCE-EQ", "BAJAJFINSV-EQ", "BPCL-EQ", "BHARTIARTL-EQ",
  "BRITANNIA-EQ", "CIPLA-EQ", "COALINDIA-EQ", "DIVISLAB-EQ", "DRREDDY-EQ", "EICHERMOT-EQ",
  "GRASIM-EQ", "HCLTECH-EQ", "HDFCBANK-EQ", "HDFCLIFE-EQ", "HEROMOTOCO-EQ", "HINDALCO-EQ",
  "HINDUNILVR-EQ", "ICICIBANK-EQ", "ITC-EQ", "INDUSINDBK-EQ", "INFY-EQ", "JSWSTEEL-EQ",
  "KOTAKBANK-EQ", "LTIM-EQ", "LT-EQ", "M&M-EQ", "MARUTI-EQ", "NTPC-EQ", "NESTLEIND-EQ",
  "ONGC-EQ", "POWERGRID-EQ", "RELIANCE-EQ", "SBILIFE-EQ", "SHRIRAMFIN-EQ", "SBIN-EQ",
  "SUNPHARMA-EQ", "TCS-EQ", "TATACONSUM-EQ", "TATAMOTORS-EQ", "TATASTEEL-EQ", "TECHM-EQ",
  "TITAN-EQ", "ULTRACEMCO-EQ", "WIPRO-EQ",
];

const tokens = [
  "25", "15083", "157", "236", "5900", "16669", "317", "16675", "526", "10604",
  "547", "694", "20374", "10940", "881", "910", "1232", "7229", "1333", "467",
  "1348", "1363", "1394", "4963", "1660", "5258", "1594", "11723", "1922", "17818",
  "11483", "2031", "10999", "11630", "17963", "2475", "14977", "2885", "21808", "4306",
  "3045", "3351", "11536", "3432", "3456", "3499", "13538", "3506", "11532", "3787",
];

const LTP_SPREADSHEET_ID = "1FNvmY09AhoraMbEG1XTSFY2ZmTX-zWR6xUKzBSGmaqs";
const LTP_RANGE = "Sheet1!A1:Z1000";
const OHLC_SPREADSHEET_ID = "1FNvmY09AhoraMbEG1XTSFY2ZmTX-zWR6xUKzBSGmaqs";

async function fetchOHLCData() {
  try {
    const totp = speakeasy.totp({
      secret: process.env.TOTP_SECRET,
      encoding: "base32",
    });

    const session = await api.generateSession(
      process.env.CLIENT_ID,
      process.env.MPIN,
      totp
    );

    if (!session || !session.data || !session.data.jwtToken) {
      console.error("Session response:", JSON.stringify(session, null, 2));
      throw new Error("Authentication failed: Invalid session response");
    }

    const jwtToken = session.data.jwtToken;

    // Get 2-hour OHLC by aggregating last 2 hourly candles
    const now = new Date();
    // Fetch last 3 hours to ensure we get at least 2 complete hourly candles
    const fromTime = new Date(now.getTime() - 3 * 60 * 60 * 1000);
    
    // Format dates as "YYYY-MM-DD HH:MM"
    const toDate = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}-${String(now.getDate()).padStart(2, '0')} ${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}`;
    const fromDate = `${fromTime.getFullYear()}-${String(fromTime.getMonth() + 1).padStart(2, '0')}-${String(fromTime.getDate()).padStart(2, '0')} ${String(fromTime.getHours()).padStart(2, '0')}:${String(fromTime.getMinutes()).padStart(2, '0')}`;
    
    const ohlcData = [];
    
    // Fetch hourly candles and aggregate last 2 to create 2-hour candles
    for (let i = 0; i < scripts.length; i++) {
      try {
        const historicParam = {
          exchange: "NSE",
          symboltoken: tokens[i],
          interval: "ONE_HOUR",
          fromdate: fromDate,
          todate: toDate
        };

        const candles = await api.getCandleData(historicParam, jwtToken);
        
        if (candles?.data && candles.data.length >= 2) {
          // Get the last 2 hourly candles to create a 2-hour candle
          const secondLastCandle = candles.data[candles.data.length - 2];
          const latestCandle = candles.data[candles.data.length - 1];
          
          // SmartAPI candle format: [timestamp, open, high, low, close, volume]
          // Aggregate 2 hours: Open from first, High/Low from both, Close from last
          const open = secondLastCandle[1];
          const high = Math.max(secondLastCandle[2], latestCandle[2]);
          const low = Math.min(secondLastCandle[3], latestCandle[3]);
          const close = latestCandle[4];
          const volume = (secondLastCandle[5] || 0) + (latestCandle[5] || 0);
          
          ohlcData.push({
            symbolToken: tokens[i],
            tradingSymbol: scripts[i],
            open: open,
            high: high,
            low: low,
            close: close,
            ltp: close, // Last traded price (close of the 2-hour period)
            volume: volume,
            averagePrice: ((open + close) / 2).toFixed(2) // Average of 2-hour open and close
          });
        } else if (candles?.data && candles.data.length === 1) {
          // Fallback: If only 1 candle available (edge case), use it
          const latestCandle = candles.data[0];
          ohlcData.push({
            symbolToken: tokens[i],
            tradingSymbol: scripts[i],
            open: latestCandle[1],
            high: latestCandle[2],
            low: latestCandle[3],
            close: latestCandle[4],
            ltp: latestCandle[4],
            volume: latestCandle[5] || 0,
            averagePrice: ((latestCandle[1] + latestCandle[4]) / 2).toFixed(2)
          });
        } else {
          console.warn(`⚠️  No candle data for ${scripts[i]}`);
        }
      } catch (err) {
        console.error(`Error fetching candle for ${scripts[i]}:`, err.message);
      }
    }
    
    return ohlcData;
  } catch (error) {
    console.error("Error fetching OHLC data:", error);
    return [];
  }
}

async function getOrCreateSheet(sheets, sheetName) {
  try {
    const spreadsheet = await sheets.spreadsheets.get({
      spreadsheetId: OHLC_SPREADSHEET_ID,
    });

    const sheet = spreadsheet.data.sheets.find(
      (s) => s.properties.title === sheetName
    );

    if (sheet) {
      return sheet.properties.sheetId;
    }

    const response = await sheets.spreadsheets.batchUpdate({
      spreadsheetId: OHLC_SPREADSHEET_ID,
      resource: {
        requests: [
          {
            addSheet: {
              properties: {
                title: sheetName,
              },
            },
          },
        ],
      },
    });

    return response.data.replies[0].addSheet.properties.sheetId;
  } catch (error) {
    console.error(`Error creating sheet ${sheetName}:`, error.message);
    return null;
  }
}

async function appendOHLCData(authClient, timestamp, ohlcArray) {
  const sheets = google.sheets({ version: "v4", auth: authClient });

  const spreadsheet = await sheets.spreadsheets.get({
    spreadsheetId: OHLC_SPREADSHEET_ID,
  });

  const existingSheets = spreadsheet.data.sheets.map((s) => s.properties.title);

  const createSheetRequests = [];
  const sheetsToCreate = [];

  scripts.forEach((script, i) => {
    const stockName = script.replace("-EQ", "");
    const sheetName = `${i + 1}. ${stockName}`;

    if (!existingSheets.includes(sheetName)) {
      createSheetRequests.push({
        addSheet: {
          properties: {
            title: sheetName,
          },
        },
      });
      sheetsToCreate.push(sheetName);
    }
  });

  if (createSheetRequests.length > 0) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: OHLC_SPREADSHEET_ID,
      resource: { requests: createSheetRequests },
    });
    console.log(`✅ Created ${createSheetRequests.length} new sheets`);
  }

  const sheet1 = spreadsheet.data.sheets.find(
    (s) => s.properties.title === "Sheet1"
  );
  if (sheet1) {
    try {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: OHLC_SPREADSHEET_ID,
        resource: {
          requests: [
            {
              deleteSheet: {
                sheetId: sheet1.properties.sheetId,
              },
            },
          ],
        },
      });
      console.log("✅ Deleted default Sheet1");
    } catch (error) {
      console.error("Error deleting default Sheet1:", error);
    }
  }

  console.log(`Writing data to ${scripts.length} sheets...`);

  for (let i = 0; i < scripts.length; i++) {
    const script = scripts[i];
    const stockName = script.replace("-EQ", "");
    const sheetName = `${i + 1}. ${stockName}`;
    const stockData = ohlcArray.find((el) => el?.tradingSymbol === script);

    if (!stockData) {
      console.log(`⚠️  ${script} data not found, skipping...`);
      continue;
    }

    const existingData = await sheets.spreadsheets.values.get({
      spreadsheetId: OHLC_SPREADSHEET_ID,
      range: `'${sheetName}'!A:G`,
    });

    const existingRows = existingData.data.values || [];
    const isNewSheet = existingRows.length === 0;

    if (isNewSheet) {
      const rows = [];
      rows.push([sheetName, "", "", "", "", "", ""]);
      rows.push(["Time", "Open", "High", "Low", "Close", "Volume", "ATP"]);
      rows.push([
        timestamp,
        stockData.open || 0,
        stockData.high || 0,
        stockData.low || 0,
        stockData.close || stockData.ltp || 0,
        stockData.tradeVolume || stockData.volume || 0,
        stockData.avgPrice || stockData.atp || 0,
      ]);

      await sheets.spreadsheets.values.update({
        spreadsheetId: OHLC_SPREADSHEET_ID,
        range: `'${sheetName}'!A1:G3`,
        valueInputOption: "RAW",
        resource: { values: rows },
      });
    } else {
      const newRow = [
        timestamp,
        stockData.open || 0,
        stockData.high || 0,
        stockData.low || 0,
        stockData.close || stockData.ltp || 0,
        stockData.tradeVolume || stockData.volume || 0,
        stockData.avgPrice || stockData.atp || 0,
      ];

      await sheets.spreadsheets.values.append({
        spreadsheetId: OHLC_SPREADSHEET_ID,
        range: `'${sheetName}'!A:G`,
        valueInputOption: "RAW",
        insertDataOption: "INSERT_ROWS",
        resource: { values: [newRow] },
      });
    }

    if ((i + 1) % 10 === 0) {
      console.log(`  Progress: ${i + 1}/${scripts.length} sheets written`);
    }
  }

  console.log(`✅ Written data to ${scripts.length} individual sheets`);

  console.log(`\n🎨 Applying formatting to all ${scripts.length} sheets...`);

  const finalSpreadsheet = await sheets.spreadsheets.get({
    spreadsheetId: OHLC_SPREADSHEET_ID,
  });

  const allFormatRequests = [];

  for (let i = 0; i < scripts.length; i++) {
    const script = scripts[i];
    const stockName = script.replace("-EQ", "");
    const sheetName = `${i + 1}. ${stockName}`;

    const sheetInfo = finalSpreadsheet.data.sheets.find(
      (s) => s.properties.title === sheetName
    );
    if (!sheetInfo) continue;

    const sheetId = sheetInfo.properties.sheetId;

    allFormatRequests.push({
      mergeCells: {
        range: {
          sheetId: sheetId,
          startRowIndex: 0,
          endRowIndex: 1,
          startColumnIndex: 0,
          endColumnIndex: 7,
        },
        mergeType: "MERGE_ALL",
      },
    });

    allFormatRequests.push({
      repeatCell: {
        range: {
          sheetId: sheetId,
          startRowIndex: 0,
          endRowIndex: 1,
          startColumnIndex: 0,
          endColumnIndex: 7,
        },
        cell: {
          userEnteredFormat: {
            textFormat: {
              bold: true,
              fontSize: 14,
              foregroundColor: {
                red: 1,
                green: 1,
                blue: 1,
              },
            },
            backgroundColor: {
              red: 0.2,
              green: 0.6,
              blue: 0.86,
            },
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
          },
        },
        fields: "userEnteredFormat",
      },
    });

    allFormatRequests.push({
      repeatCell: {
        range: {
          sheetId: sheetId,
          startRowIndex: 1,
          endRowIndex: 2,
          startColumnIndex: 0,
          endColumnIndex: 7,
        },
        cell: {
          userEnteredFormat: {
            textFormat: {
              bold: true,
              fontSize: 10,
            },
            backgroundColor: {
              red: 0.85,
              green: 0.85,
              blue: 0.85,
            },
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
          },
        },
        fields: "userEnteredFormat",
      },
    });

    allFormatRequests.push({
      repeatCell: {
        range: {
          sheetId: sheetId,
          startRowIndex: 2,
          endRowIndex: 1000,
          startColumnIndex: 0,
          endColumnIndex: 7,
        },
        cell: {
          userEnteredFormat: {
            horizontalAlignment: "CENTER",
            verticalAlignment: "MIDDLE",
            textFormat: {
              bold: false,
              fontSize: 10,
            },
          },
        },
        fields: "userEnteredFormat",
      },
    });

    allFormatRequests.push({
      updateDimensionProperties: {
        range: {
          sheetId: sheetId,
          dimension: "COLUMNS",
          startIndex: 0,
          endIndex: 1,
        },
        properties: {
          pixelSize: 150,
        },
        fields: "pixelSize",
      },
    });

    for (let col = 1; col < 7; col++) {
      allFormatRequests.push({
        updateDimensionProperties: {
          range: {
            sheetId: sheetId,
            dimension: "COLUMNS",
            startIndex: col,
            endIndex: col + 1,
          },
          properties: {
            pixelSize: 100,
          },
          fields: "pixelSize",
        },
      });
    }

    if ((i + 1) % 10 === 0) {
      console.log(`  Formatting progress: ${i + 1}/${scripts.length} sheets`);
    }
  }

  if (allFormatRequests.length > 0) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: OHLC_SPREADSHEET_ID,
      resource: { requests: allFormatRequests },
    });
    console.log(
      `✅ Formatted all ${scripts.length} sheets with centered cells and colors`
    );
  }

  console.log(`📝 Sheets created successfully`);
  console.log(`📌 Check the tabs at the bottom of your spreadsheet!`);
  console.log(`🔄 If formatting looks missing, hard refresh: Cmd+Shift+R (Mac) or Ctrl+Shift+R (Windows)`);

  const sheet1Final = finalSpreadsheet.data.sheets.find(
    (s) => s.properties.title === "Sheet1"
  );
  if (sheet1Final) {
    try {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: OHLC_SPREADSHEET_ID,
        resource: {
          requests: [
            {
              deleteSheet: {
                sheetId: sheet1Final.properties.sheetId,
              },
            },
          ],
        },
      });
      console.log("✅ Deleted default Sheet1");
    } catch (error) {
      console.error("Error deleting default Sheet1:", error);
    }
  }
}

function getISTTimestamp() {
  const now = new Date();
  
  const istTime = now.toLocaleString("en-IN", {
    timeZone: "Asia/Kolkata",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  
  return istTime;
}

async function fetchAndUpdateOHLC() {
  try {
    const ohlcData = await fetchOHLCData();
    const authClient = await auth.getClient();
    const timestamp = getISTTimestamp(); 

    await appendOHLCData(authClient, timestamp, ohlcData);
  } catch (error) {
    console.error("Error in fetchAndUpdateOHLC:", error);
  }
}

const NSE_HOLIDAYS_2025 = [
  "26/1/2025", "26/2/2025", "14/3/2025", "29/3/2025", "10/4/2025",
  "14/4/2025", "1/5/2025", "16/6/2025", "15/8/2025", "27/8/2025",
  "2/10/2025", "1/11/2025", "5/11/2025", "15/11/2025", "25/12/2025",
];

function isMarketHoliday() {
  const now = new Date();
  const today = now.toLocaleDateString("en-IN", { timeZone: "Asia/Kolkata" });
  return NSE_HOLIDAYS_2025.includes(today);
}

async function fetchAndUpdateAll() {
  const now = new Date();
  const istTime = now.toLocaleString("en-IN", {
    timeZone: "Asia/Kolkata",
    hour12: false,
  });

  console.log(`🕐 Cron triggered at: ${istTime} IST`);

  if (isMarketHoliday()) {
    console.log(
      "⏸️  SKIPPED: Today is a market holiday. Stock market is closed."
    );
    return;
  }

  console.log("✅ Market is open. Fetching data...\n");

  try {
    await fetchAndUpdateOHLC();
    console.log("\n✅ Data fetch completed successfully!");
  } catch (error) {
    console.error("\n❌ Data fetch failed:", error.message);
  }

}

// PRODUCTION CRON SCHEDULES

// 1. First Update: 10:15 AM IST (1 hour after market open) - Monday-Friday
//    Uses 9:00-10:00 AM candle (includes market open at 9:15)
cron.schedule("15 10 * * 1-5", fetchAndUpdateAll, {
  scheduled: true,
  timezone: "Asia/Kolkata",
});

// 2. Second Update: 12:15 PM IST (2 hours later) - Monday-Friday
//    Aggregates 10:00-11:00 + 11:00-12:00 candles = 2-hour OHLC
cron.schedule("15 12 * * 1-5", fetchAndUpdateAll, {
  scheduled: true,
  timezone: "Asia/Kolkata",
});

// 3. Third Update: 2:15 PM IST (2 hours later) - Monday-Friday
//    Aggregates 12:00-1:00 + 1:00-2:00 candles = 2-hour OHLC
cron.schedule("15 14 * * 1-5", fetchAndUpdateAll, {
  scheduled: true,
  timezone: "Asia/Kolkata",
});

// 4. Final Update: 3:30 PM IST (market close) - Monday-Friday
//    Aggregates 2:00-3:00 + 3:00-3:30 candles (or uses latest available)
cron.schedule("30 15 * * 1-5", fetchAndUpdateAll, {
  scheduled: true,
  timezone: "Asia/Kolkata",
});

console.log("\n" + "=".repeat(80));
console.log("🚀 Smart Algo OHLC Tracker - PRODUCTION MODE");
console.log("=".repeat(80));
console.log("📊 Tracking: 50 stocks across individual sheets");
console.log("⏰ Schedule: Monday-Friday, 4 updates during market hours");
console.log("🛡️  Protection: Holiday skip (NSE holidays)");
console.log("📅 Updates: 10:15 AM, 12:15 PM, 2:15 PM, 3:30 PM IST");
console.log("📈 Data: 2-hour aggregated OHLC candles");
console.log("🔗 Spreadsheet: https://docs.google.com/spreadsheets/d/1FNvmY09AhoraMbEG1XTSFY2ZmTX-zWR6xUKzBSGmaqs/edit");
console.log("=".repeat(80) + "\n");
