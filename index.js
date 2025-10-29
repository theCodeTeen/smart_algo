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

    // Market-aligned hourly candles: 9:15 AM to 3:30 PM
    // Candle structure: 9:15-10:15, 10:15-11:15, 11:15-12:15, 12:15-1:15, 1:15-2:15, 2:15-3:15, 3:15-3:30
    const now = new Date();
    const istTime = now.toLocaleString("en-IN", {
      timeZone: "Asia/Kolkata",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
    
    const [hourStr, minuteStr] = istTime.split(":");
    const currentHour = parseInt(hourStr);
    const currentMinute = parseInt(minuteStr);
    
    let hourStart, hourEnd;
    
    // Special handling for 3:29 PM - use 4:15 instead (since 3:15 already gets hourly data)
    if (currentHour === 15 && currentMinute === 29) {
      // Fetch 3:15-4:15 using ONE_HOUR interval
      const istDateStr = now.toLocaleString("en-IN", {
        timeZone: "Asia/Kolkata",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
      });
      const [day, month, year] = istDateStr.split("/");
      hourStart = new Date(`${year}-${month}-${day}T15:15:00+05:30`);
      hourEnd = new Date(`${year}-${month}-${day}T16:15:00+05:30`); // 4:15 PM (next hour)
    } else {
      // Market-aligned hourly candles starting from 9:15 AM
      // At 10:14, we want the candle that STARTED at 9:15 (not 10:15)
      // Request from 9:15 to 10:15 to ensure we get the full candle data
      // Then we'll filter to get only the candle starting at 9:15
      // At 10:14 → fetch candle that started at 9:15
      // At 11:14 → fetch candle that started at 10:15
      // At 12:14 → fetch candle that started at 11:15
      // At 1:14 → fetch candle that started at 12:15
      // At 2:14 → fetch candle that started at 1:15
      // At 3:14 → fetch candle that started at 2:15
      
      // Get IST date string (YYYY-MM-DD format)
      const istDateStr = now.toLocaleString("en-IN", {
        timeZone: "Asia/Kolkata",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
      });
      const [day, month, year] = istDateStr.split("/");
      
      // Previous hour's start time (market-aligned at :15)
      const prevHour = currentHour - 1;
      
      // Construct date string in IST timezone
      // Request up to current hour:15 to get complete candle data
      hourStart = new Date(`${year}-${month}-${day}T${String(prevHour).padStart(2, '0')}:15:00+05:30`);
      hourEnd = new Date(`${year}-${month}-${day}T${String(currentHour).padStart(2, '0')}:15:00+05:30`); // Request up to current hour:15
    }
    
    // Format dates as "YYYY-MM-DD HH:MM" in IST timezone
    const formatISTDate = (date) => {
      const istStr = date.toLocaleString("en-IN", {
        timeZone: "Asia/Kolkata",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
      });
      // Convert "DD/MM/YYYY, HH:MM" to "YYYY-MM-DD HH:MM"
      const [datePart, timePart] = istStr.split(", ");
      const [day, month, year] = datePart.split("/");
      return `${year}-${month}-${day} ${timePart}`;
    };
    
    const fromDate = formatISTDate(hourStart);
    const toDate = formatISTDate(hourEnd);
    
    console.log(`📊 Fetching candle: ${fromDate} to ${toDate}`);
    
    const ohlcData = [];
    
    // Fetch latest hourly candle for each stock
    for (let i = 0; i < scripts.length; i++) {
      try {
        // Use ONE_HOUR interval for all candles (including 3:29 PM which fetches 3:15-4:15)
        const interval = "ONE_HOUR";
        
        const historicParam = {
          exchange: "NSE",
          symboltoken: tokens[i],
          interval: interval,
          fromdate: fromDate,
          todate: toDate
        };

        const candles = await api.getCandleData(historicParam, jwtToken);
        
        if (candles?.data && candles.data.length > 0) {
          // Find the candle that starts EXACTLY at our expected start time
          // Candle format: [timestamp, open, high, low, close, volume]
          // timestamp is in milliseconds (or seconds)
          const expectedStartTime = hourStart.getTime();
          
          // Find the candle that starts at our exact expected time
          let selectedCandle = null;
          
          for (const candle of candles.data) {
            const candleStartTime = candle[0]; // timestamp
            // Check if timestamp is in seconds (10 digits) or milliseconds (13 digits)
            const candleTime = candleStartTime < 10000000000 
              ? candleStartTime * 1000  // Convert seconds to milliseconds
              : candleStartTime;
            
            // Find candle that starts exactly at our expected time (within 1 minute tolerance)
            const timeDiff = Math.abs(candleTime - expectedStartTime);
            if (timeDiff <= 60 * 1000) { // 1 minute tolerance
              selectedCandle = candle;
              break; // Found exact match, stop searching
            }
          }
          
          // If no exact match, try to find the one that starts BEFORE our current time
          // (this ensures we get the previous hour's candle, not next hour's)
          if (!selectedCandle) {
            for (const candle of candles.data) {
              const candleStartTime = candle[0];
              const candleTime = candleStartTime < 10000000000 
                ? candleStartTime * 1000 
                : candleStartTime;
              
              // Take the candle that starts closest BEFORE our expected time
              if (candleTime <= expectedStartTime) {
                if (!selectedCandle || candleTime > (selectedCandle[0] < 10000000000 ? selectedCandle[0] * 1000 : selectedCandle[0])) {
                  selectedCandle = candle;
                }
              }
            }
          }
          
          // Last resort: take first candle
          if (!selectedCandle && candles.data.length > 0) {
            selectedCandle = candles.data[0];
          }
          
          if (!selectedCandle) {
            console.warn(`⚠️  No matching candle found for ${scripts[i]}`);
            continue;
          }
          
          const latestCandle = selectedCandle;
          
          // SmartAPI candle format: [timestamp, open, high, low, close, volume]
          ohlcData.push({
            symbolToken: tokens[i],
            tradingSymbol: scripts[i],
            open: latestCandle[1],
            high: latestCandle[2],
            low: latestCandle[3],
            close: latestCandle[4],
            ltp: latestCandle[4], // Last traded price (close of the hour)
            volume: latestCandle[5] || 0,
            averagePrice: ((latestCandle[1] + latestCandle[4]) / 2).toFixed(2) // Average of hourly open and close
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
        stockData.averagePrice || stockData.avgPrice || stockData.atp || 0,
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
        stockData.averagePrice || stockData.avgPrice || stockData.atp || 0,
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

// Market-aligned hourly candles starting from 9:15 AM
// 1. 10:14 AM - Fetch 9:15-10:15 candle (market-aligned, 1 hour)
cron.schedule("14 10 * * 1-5", fetchAndUpdateAll, {
  scheduled: true,
  timezone: "Asia/Kolkata",
});

// 2. 11:14 AM - Fetch 10:15-11:15 candle (market-aligned, 1 hour)
cron.schedule("14 11 * * 1-5", fetchAndUpdateAll, {
  scheduled: true,
  timezone: "Asia/Kolkata",
});

// 3. 12:14 PM - Fetch 11:15-12:15 candle (market-aligned, 1 hour)
cron.schedule("14 12 * * 1-5", fetchAndUpdateAll, {
  scheduled: true,
  timezone: "Asia/Kolkata",
});

// 4. 1:14 PM - Fetch 12:15-1:15 candle (market-aligned, 1 hour)
cron.schedule("14 13 * * 1-5", fetchAndUpdateAll, {
  scheduled: true,
  timezone: "Asia/Kolkata",
});

// 5. 2:14 PM - Fetch 1:15-2:15 candle (market-aligned, 1 hour)
cron.schedule("14 14 * * 1-5", fetchAndUpdateAll, {
  scheduled: true,
  timezone: "Asia/Kolkata",
});

// 6. 3:14 PM - Fetch 2:15-3:15 candle (market-aligned, 1 hour)
cron.schedule("14 15 * * 1-5", fetchAndUpdateAll, {
  scheduled: true,
  timezone: "Asia/Kolkata",
});

// 7. 3:29 PM - Fetch 3:15-4:15 candle (instead of 3:29, use 4:15 since 3:15 already gets hourly data)
cron.schedule("29 15 * * 1-5", fetchAndUpdateAll, {
  scheduled: true,
  timezone: "Asia/Kolkata",
});


console.log("\n" + "=".repeat(80));
console.log("🚀 Smart Algo OHLC Tracker - PRODUCTION MODE");
console.log("=".repeat(80));
console.log("📊 Tracking: 50 stocks across individual sheets");
console.log("⏰ Schedule: Monday-Friday, market-aligned hourly candles");
console.log("🛡️  Protection: Holiday skip (NSE holidays)");
console.log("📅 Updates: 10:14, 11:14, 12:14, 1:14, 2:14, 3:14, 3:29 PM IST");
console.log("📈 Market Hours: 9:15 AM - 3:30 PM IST");
console.log("🕯️  Candle Structure: 9:15-10:14, 10:15-11:14, 11:15-12:14, 12:15-1:14, 1:15-2:14, 2:15-3:14, 3:15-4:15 (market closes at 3:30)");
console.log("🔗 Spreadsheet: https://docs.google.com/spreadsheets/d/1FNvmY09AhoraMbEG1XTSFY2ZmTX-zWR6xUKzBSGmaqs/edit");
console.log("=".repeat(80) + "\n");
