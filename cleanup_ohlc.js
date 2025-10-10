const { google } = require("googleapis");
const dotenv = require("dotenv");
dotenv.config();

const auth = new google.auth.GoogleAuth({
  keyFile: "/Users/jaydeepzala/Dev/Projects/smart_algo/smart-algo-474617-07f7458ed064.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const OHLC_SPREADSHEET_ID = "1FNvmY09AhoraMbEG1XTSFY2ZmTX-zWR6xUKzBSGmaqs";

async function cleanupSpreadsheet() {
  try {
    const authClient = await auth.getClient();
    const sheets = google.sheets({ version: "v4", auth: authClient });

    console.log("🧹 Cleaning up OHLC spreadsheet - removing ALL sheets...\n");

    // Get all existing sheets
    const spreadsheet = await sheets.spreadsheets.get({
      spreadsheetId: OHLC_SPREADSHEET_ID,
    });

    const existingSheets = spreadsheet.data.sheets;
    console.log(`Found ${existingSheets.length} sheets to process`);

    // Delete all sheets except we need to keep at least one
    const deleteRequests = [];
    
    existingSheets.forEach((sheet, index) => {
      // Skip the first sheet, we'll clear it instead
      if (index > 0) {
        deleteRequests.push({
          deleteSheet: {
            sheetId: sheet.properties.sheetId,
          },
        });
      }
    });

    // Delete all extra sheets (keep position 0, Google Sheets requires at least 1 sheet)
    if (deleteRequests.length > 0) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: OHLC_SPREADSHEET_ID,
        resource: { requests: deleteRequests },
      });
      console.log(`✅ Deleted ${deleteRequests.length} sheets`);
    } else {
      console.log("ℹ️  No sheets to delete (keeping position 0 as required by Google Sheets)");
    }

    // Clear and reset the first sheet
    const firstSheet = existingSheets[0];
    const cleanupRequests = [
      {
        unmergeCells: {
          range: { sheetId: firstSheet.properties.sheetId },
        },
      },
      {
        repeatCell: {
          range: {
            sheetId: firstSheet.properties.sheetId,
            startRowIndex: 0,
            endRowIndex: 1000,
            startColumnIndex: 0,
            endColumnIndex: 350,
          },
          cell: {},
          fields: "userEnteredFormat",
        },
      },
      {
        updateSheetProperties: {
          properties: {
            sheetId: firstSheet.properties.sheetId,
            title: "Sheet1",
          },
          fields: "title",
        },
      },
    ];

    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: OHLC_SPREADSHEET_ID,
      resource: { requests: cleanupRequests },
    });
    console.log("✅ Cleared formatting from remaining sheet");
    console.log("✅ Renamed remaining sheet to 'Sheet1'");

    // Clear all values from the first sheet
    await sheets.spreadsheets.values.clear({
      spreadsheetId: OHLC_SPREADSHEET_ID,
      range: "Sheet1",
    });
    console.log("✅ Cleared all values");

    console.log("\n📊 Your OHLC spreadsheet is now completely empty!");
    console.log("   - All stock sheets deleted");
    console.log("   - Only one blank sheet remains");
    console.log("\n🚀 Next step: Run 'node index.js' to create 50 sheets (one per stock)\n");

  } catch (error) {
    console.error("❌ Error cleaning spreadsheet:", error.message);
  }
}

cleanupSpreadsheet();

