/**
 * Moneyview IVR Scraper – LOOP MODE
 * Optimized for Render Deployment with Chrome fix
 */

const puppeteer = require("puppeteer");
const mysql = require("mysql2/promise");

/* ===============================
   CONFIGURATION - EDIT THESE VALUES
================================ */
// Database Configuration
const DB_CONFIG = {
    host: "82.25.121.2",
    user: "u527886566_scraper_db",
    password: "VAKILr6762",
    database: "u527886566_scraper_db",
    port: 3306,
    waitForConnections: true,
    connectionLimit: 5,
    enableKeepAlive: true,
    keepAliveInitialDelay: 0
};

// Moneyview Login Credentials
const LOGIN_CREDENTIALS = {
    email: "admin@switchmyloan.in",
    password: "Admin@123"
};

// Scraper Settings
const SCRAPE_INTERVAL_MS = 20 * 1000; // 20 seconds
const MAX_ERRORS = 5;
const LOGIN_URL = "https://mv-dashboard.switchmyloan.in/login";
const DATA_URL  = "https://mv-dashboard.switchmyloan.in/mv-ivr-logs";

/* ===============================
   COLUMN INDEXES
================================ */
const IDX_SN      = 0;
const IDX_NAME    = 1;
const IDX_MSG     = 2;
const IDX_NUMBER  = 3;
const IDX_PAN     = 4;
const IDX_SALARY  = 5;
const IDX_DOB     = 6;
const IDX_CREATED = 7;

/* ===============================
   HELPER FUNCTIONS
================================ */
const sleep = ms => new Promise(r => setTimeout(r, ms));

function log(msg, type = "INFO") {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] [${type}] ${msg}`);
}

function parseDate(val) {
    if (!val) return null;
    val = val.trim();

    const iso = new Date(val);
    if (!isNaN(iso)) return iso.toISOString().split("T")[0];

    const m = val.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (m) {
        const d = new Date(m[3], m[2] - 1, m[1]);
        if (!isNaN(d)) return d.toISOString().split("T")[0];
    }
    
    return null;
}

/* ===============================
   DATABASE INITIALIZATION
================================ */
async function initDB() {
    log(`Connecting to database at ${DB_CONFIG.host}...`);

    const pool = await mysql.createPool(DB_CONFIG);

    // Test connection
    await pool.query('SELECT 1');
    
    // Create table if not exists
    await pool.execute(`
        CREATE TABLE IF NOT EXISTS ivr_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sn VARCHAR(50),
            full_name VARCHAR(255),
            moneyview_msg TEXT,
            phone_number VARCHAR(20),
            pan_card VARCHAR(20),
            salary VARCHAR(100),
            dob_raw VARCHAR(50),
            dob DATE,
            created VARCHAR(50),
            scrape_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY unique_record (phone_number, pan_card, created)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    log("Database connection established and table verified", "SUCCESS");
    return pool;
}

/* ===============================
   BROWSER SETUP for Render - FIXED
================================ */
async function createBrowser() {
    log("Launching browser...");
    
    const launchOptions = {
        headless: "new",
        args: [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--single-process",
            "--no-zygote",
            "--disable-features=site-per-process"
        ],
        ignoreHTTPSErrors: true,
        timeout: 30000
    };

    // Try different Chrome paths on Render
    const chromePaths = [
        '/usr/bin/chromium-browser',
        '/usr/bin/chromium',
        '/usr/bin/google-chrome-stable',
        '/usr/bin/google-chrome',
        '/opt/google/chrome/chrome'
    ];

    for (const path of chromePaths) {
        try {
            const fs = require('fs');
            if (fs.existsSync(path)) {
                launchOptions.executablePath = path;
                log(`Found Chrome at: ${path}`);
                break;
            }
        } catch (e) {
            // Continue to next path
        }
    }

    // If no Chrome found, let puppeteer download it
    if (!launchOptions.executablePath) {
        log("No system Chrome found, puppeteer will download Chrome", "WARN");
    }

    try {
        const browser = await puppeteer.launch(launchOptions);
        const version = await browser.version();
        log(`Browser launched successfully: ${version}`, "SUCCESS");
        return browser;
    } catch (error) {
        log(`Failed to launch browser: ${error.message}`, "ERROR");
        
        // Try one more time without specifying executable path
        if (launchOptions.executablePath) {
            log("Retrying without specific executable path...", "WARN");
            delete launchOptions.executablePath;
            try {
                const browser = await puppeteer.launch(launchOptions);
                log("Browser launched using puppeteer's bundled Chrome", "SUCCESS");
                return browser;
            } catch (retryError) {
                log(`Retry also failed: ${retryError.message}`, "ERROR");
            }
        }
        
        throw error;
    }
}

/* ===============================
   LOGIN FUNCTION
================================ */
async function login(page) {
    log(`Logging in to ${LOGIN_URL}...`);
    
    try {
        await page.goto(LOGIN_URL, { 
            waitUntil: "networkidle0", 
            timeout: 60000 
        });

        // Wait for login form
        await page.waitForSelector('input[name="email"]', { timeout: 30000 });

        log("Entering credentials...");
        await page.type('input[name="email"]', LOGIN_CREDENTIALS.email, { delay: 30 });
        await page.type('input[name="password"]', LOGIN_CREDENTIALS.password, { delay: 30 });

        log("Submitting login form...");
        await Promise.all([
            page.click('button[type="submit"]'),
            page.waitForNavigation({ waitUntil: "networkidle0", timeout: 60000 })
        ]);

        // Check if login was successful
        await sleep(2000);
        const currentUrl = page.url();
        
        if (currentUrl.includes('dashboard') || currentUrl.includes('mv-ivr-logs')) {
            log("Login successful!", "SUCCESS");
            return true;
        } else {
            throw new Error(`Login may have failed. Current URL: ${currentUrl}`);
        }
    } catch (error) {
        log(`Login failed: ${error.message}`, "ERROR");
        throw error;
    }
}

/* ===============================
   SCRAPE FUNCTION
================================ */
async function scrape(page, pool) {
    log(`Navigating to ${DATA_URL}...`);
    
    try {
        await page.goto(DATA_URL, { 
            waitUntil: "networkidle0", 
            timeout: 60000 
        });

        // Wait for table
        await page.waitForSelector("tbody tr", { timeout: 30000 });
        await sleep(2000);

        const rows = await page.evaluate(() => {
            const rows = [];
            const tableRows = document.querySelectorAll("tbody tr");
            
            tableRows.forEach(tr => {
                const cells = tr.querySelectorAll("td");
                const rowData = Array.from(cells).map(cell => {
                    let text = cell.textContent || cell.innerText;
                    text = text.replace(/\s+/g, ' ').trim();
                    return text;
                });
                
                if (rowData.length >= 8) {
                    rows.push(rowData);
                }
            });
            
            return rows;
        });

        log(`Found ${rows.length} rows`);

        if (rows.length === 0) {
            log("No data found", "WARN");
            return { inserted: 0, duplicates: 0, errors: 0 };
        }

        let inserted = 0;
        let duplicates = 0;
        let errors = 0;

        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            
            try {
                const result = await pool.execute(`
                    INSERT INTO ivr_logs
                    (sn, full_name, moneyview_msg, phone_number, pan_card, salary, dob_raw, dob, created)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE scrape_timestamp = NOW()
                `, [
                    row[IDX_SN] || '',
                    row[IDX_NAME] || '',
                    row[IDX_MSG] || '',
                    row[IDX_NUMBER] || '',
                    row[IDX_PAN] || '',
                    row[IDX_SALARY] || '',
                    row[IDX_DOB] || '',
                    parseDate(row[IDX_DOB]),
                    row[IDX_CREATED] || ''
                ]);

                if (result[0].affectedRows > 0) {
                    inserted++;
                } else {
                    duplicates++;
                }
            } catch (error) {
                if (error.code === 'ER_DUP_ENTRY') {
                    duplicates++;
                } else {
                    errors++;
                }
            }
        }

        log(`Inserted: ${inserted} | Duplicates: ${duplicates} | Errors: ${errors}`, "SUCCESS");
        return { inserted, duplicates, errors };
        
    } catch (error) {
        log(`Scrape error: ${error.message}`, "ERROR");
        throw error;
    }
}

/* ===============================
   MAIN APPLICATION
================================ */
(async () => {
    let browser = null;
    let page = null;
    let pool = null;
    let errorCount = 0;
    let isShuttingDown = false;

    async function cleanup(exit = false) {
        if (isShuttingDown) return;
        isShuttingDown = true;
        
        log("Cleaning up...");
        
        try {
            if (page && !page.isClosed()) await page.close();
        } catch (e) {}
        
        try {
            if (browser) await browser.close();
        } catch (e) {}
        
        try {
            if (pool) await pool.end();
        } catch (e) {}
        
        if (exit) {
            log("Exiting...");
            setTimeout(() => process.exit(0), 100);
        }
    }

    process.on('SIGINT', () => cleanup(true));
    process.on('SIGTERM', () => cleanup(true));

    try {
        log("========================================");
        log("MONEYVIEW SCRAPER STARTING");
        log("========================================");
        
        log(`Database: ${DB_CONFIG.host}/${DB_CONFIG.database}`);
        log(`Login: ${LOGIN_CREDENTIALS.email}`);
        log(`Interval: ${SCRAPE_INTERVAL_MS/1000}s`);
        
        // Initialize
        pool = await initDB();
        browser = await createBrowser();
        page = await browser.newPage();
        
        // Configure page
        await page.setViewport({ width: 1366, height: 768 });
        await page.setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        
        // Login
        await login(page);
        
        log("READY - Starting main loop");
        
        // Main loop
        while (true) {
            try {
                await scrape(page, pool);
                errorCount = 0;
                
                log(`Sleeping ${SCRAPE_INTERVAL_MS/1000}s...`);
                await sleep(SCRAPE_INTERVAL_MS);
                
            } catch (error) {
                errorCount++;
                log(`Error ${errorCount}/${MAX_ERRORS}: ${error.message}`, "ERROR");
                
                if (errorCount >= MAX_ERRORS) {
                    log("Too many errors, restarting...", "WARN");
                    await cleanup(false);
                    
                    // Reinitialize
                    browser = await createBrowser();
                    page = await browser.newPage();
                    await page.setViewport({ width: 1366, height: 768 });
                    await page.setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
                    await login(page);
                    
                    errorCount = 0;
                } else {
                    await sleep(5000);
                }
            }
        }
        
    } catch (fatalError) {
        log(`FATAL: ${fatalError.message}`, "CRITICAL");
        await cleanup(true);
    }
})();
