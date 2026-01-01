/**
 * Moneyview IVR Scraper – LOOP MODE
 * All credentials and config embedded in code
 * Optimized for Render Deployment
 */

const puppeteer = require("puppeteer");
const mysql = require("mysql2/promise");

/* ===============================
   CONFIGURATION - EDIT THESE VALUES
================================ */
// Database Configuration
const DB_CONFIG = {
    host: "82.25.121.2",        // Your database host
    user: "u563444031_ruser",   // Your database username
    password: "Root@123",       // Your database password
    database: "u563444031_ruby", // Your database name
    port: 3306,
    waitForConnections: true,
    connectionLimit: 5,
    enableKeepAlive: true,
    keepAliveInitialDelay: 0
};

// Moneyview Login Credentials
const LOGIN_CREDENTIALS = {
    email: "mkmassociates234@gmail.com",    // Your login email
    password: "Z9r?Qn#H8p$L"               // Your login password
};

// Scraper Settings
const SCRAPE_INTERVAL_MS = 20 * 1000; // 20 seconds
const MAX_ERRORS = 5;
const LOGIN_URL = "https://mv-dashboard.switchmyloan.in/login";
const DATA_URL  = "https://mv-dashboard.switchmyloan.in/mv-ivr-logs";

/* ===============================
   COLUMN INDEXES (Don't change unless table structure changes)
================================ */
const IDX_SN      = 0;  // Serial Number
const IDX_NAME    = 1;  // Full Name
const IDX_MSG     = 2;  // Moneyview Message
const IDX_NUMBER  = 3;  // Phone Number
const IDX_PAN     = 4;  // PAN Card
const IDX_SALARY  = 5;  // Salary
const IDX_DOB     = 6;  // Date of Birth (raw)
const IDX_CREATED = 7;  // Created Date

/* ===============================
   HELPER FUNCTIONS
================================ */
const sleep = ms => new Promise(r => setTimeout(r, ms));

function log(msg, type = "INFO") {
    const timestamp = new Date().toISOString();
    const typeColor = {
        'INFO': '\x1b[36m',    // Cyan
        'SUCCESS': '\x1b[32m', // Green
        'WARN': '\x1b[33m',    // Yellow
        'ERROR': '\x1b[31m',   // Red
        'CRITICAL': '\x1b[41m\x1b[37m' // Red background, white text
    }[type] || '\x1b[0m';
    
    console.log(`\x1b[90m[${timestamp}]\x1b[0m ${typeColor}[${type}]\x1b[0m ${msg}`);
}

function parseDate(val) {
    if (!val) return null;
    val = val.trim();

    // Try ISO date first
    const iso = new Date(val);
    if (!isNaN(iso)) return iso.toISOString().split("T")[0];

    // Try DD/MM/YYYY format
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
   BROWSER SETUP for Render
================================ */
async function createBrowser() {
    log("Launching Chrome browser...");
    
    const launchOptions = {
        headless: "new",
        args: [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--single-process",
            "--no-zygote",
            "--disable-features=site-per-process",
            "--disable-accelerated-2d-canvas",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-breakpad",
            "--disable-component-extensions-with-background-pages",
            "--disable-extensions",
            "--disable-features=TranslateUI",
            "--disable-ipc-flooding-protection",
            "--disable-renderer-backgrounding",
            "--window-size=1366,768"
        ],
        ignoreHTTPSErrors: true,
        timeout: 30000
    };

    // On Render, use system Chrome
    if (process.env.RENDER) {
        launchOptions.executablePath = process.env.CHROME_PATH || '/usr/bin/chromium';
        log(`Using Chrome executable at: ${launchOptions.executablePath}`);
    }

    try {
        const browser = await puppeteer.launch(launchOptions);
        const version = await browser.version();
        log(`Browser launched successfully: ${version}`, "SUCCESS");
        return browser;
    } catch (error) {
        log(`Failed to launch browser: ${error.message}`, "ERROR");
        
        // If Chrome not found, try to provide helpful message
        if (error.message.includes('Could not find Chrome')) {
            log("Chrome not found. Make sure to run 'npx puppeteer browsers install chrome' in build command", "CRITICAL");
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
        await page.waitForSelector('input[name="password"]', { timeout: 30000 });

        log("Entering credentials...");
        // Enter email
        await page.type('input[name="email"]', LOGIN_CREDENTIALS.email, { delay: 30 });
        // Enter password
        await page.type('input[name="password"]', LOGIN_CREDENTIALS.password, { delay: 30 });

        log("Submitting login form...");
        // Click submit and wait for navigation
        await Promise.all([
            page.click('button[type="submit"]'),
            page.waitForNavigation({ waitUntil: "networkidle0", timeout: 60000 })
        ]);

        // Verify login success
        const currentUrl = page.url();
        if (currentUrl.includes('dashboard') || currentUrl.includes('mv-ivr-logs')) {
            log("Login successful! Redirected to dashboard", "SUCCESS");
            return true;
        } else {
            // Check for login errors
            const errorText = await page.evaluate(() => {
                const errorEl = document.querySelector('.error, .alert-danger, [class*="error"]');
                return errorEl ? errorEl.textContent.trim() : null;
            }).catch(() => null);
            
            if (errorText) {
                throw new Error(`Login failed: ${errorText}`);
            } else {
                throw new Error(`Login may have failed. Current URL: ${currentUrl}`);
            }
        }
    } catch (error) {
        log(`Login failed: ${error.message}`, "ERROR");
        // Take screenshot for debugging
        try {
            await page.screenshot({ 
                path: '/tmp/login-error.png',
                fullPage: true 
            });
            log("Screenshot saved to /tmp/login-error.png", "INFO");
        } catch (screenshotError) {
            // Ignore screenshot errors
        }
        throw error;
    }
}

/* ===============================
   SCRAPE FUNCTION
================================ */
async function scrape(page, pool) {
    log(`Navigating to IVR logs at ${DATA_URL}...`);
    
    try {
        await page.goto(DATA_URL, { 
            waitUntil: "networkidle0", 
            timeout: 60000 
        });

        // Wait for table to load
        log("Waiting for data table to load...");
        await page.waitForSelector("tbody tr", { timeout: 30000 });
        
        // Give a moment for JavaScript to render
        await sleep(2000);

        const rows = await page.evaluate(() => {
            const rows = [];
            const tableRows = document.querySelectorAll("tbody tr");
            
            tableRows.forEach(tr => {
                const cells = tr.querySelectorAll("td");
                const rowData = Array.from(cells).map(cell => {
                    let text = cell.textContent || cell.innerText;
                    // Clean up whitespace
                    text = text.replace(/\s+/g, ' ').trim();
                    return text;
                });
                
                if (rowData.length >= 8) {
                    rows.push(rowData);
                }
            });
            
            return rows;
        });

        log(`Found ${rows.length} data rows to process`);

        if (rows.length === 0) {
            log("No data found in table. The table might be empty or selectors changed.", "WARN");
            return { inserted: 0, duplicates: 0, errors: 0 };
        }

        let inserted = 0;
        let duplicates = 0;
        let errors = 0;

        log("Processing rows and inserting into database...");
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            
            // Log first row for debugging
            if (i === 0) {
                log(`First row sample: ${row.slice(0, 3).join(' | ')}...`, "DEBUG");
            }
            
            try {
                const result = await pool.execute(`
                    INSERT INTO ivr_logs
                    (sn, full_name, moneyview_msg, phone_number, pan_card, salary, dob_raw, dob, created)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON DUPLICATE KEY UPDATE 
                        sn = VALUES(sn),
                        full_name = VALUES(full_name),
                        moneyview_msg = VALUES(moneyview_msg),
                        salary = VALUES(salary),
                        dob_raw = VALUES(dob_raw),
                        dob = VALUES(dob),
                        scrape_timestamp = NOW()
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
                    log(`DB Error for row ${i + 1}: ${error.message}`, "ERROR");
                }
            }
        }

        log(`Database Update - Inserted: ${inserted} | Duplicates: ${duplicates} | Errors: ${errors}`, "SUCCESS");
        return { inserted, duplicates, errors };
        
    } catch (error) {
        log(`Scrape error: ${error.message}`, "ERROR");
        
        // Take screenshot for debugging
        try {
            await page.screenshot({ 
                path: '/tmp/scrape-error.png',
                fullPage: true 
            });
            log("Screenshot saved to /tmp/scrape-error.png", "INFO");
        } catch (screenshotError) {
            // Ignore screenshot errors
        }
        
        throw error;
    }
}

/* ===============================
   MAIN APPLICATION LOOP
================================ */
(async () => {
    let browser = null;
    let page = null;
    let pool = null;
    let errorCount = 0;
    let isShuttingDown = false;
    let scrapeCount = 0;

    // Cleanup function
    async function cleanup(exit = false) {
        if (isShuttingDown) return;
        isShuttingDown = true;
        
        log("Cleaning up resources...");
        
        const cleanupTasks = [];
        
        if (page && !page.isClosed()) {
            cleanupTasks.push(page.close().catch(e => log(`Page close error: ${e.message}`, "DEBUG")));
        }
        
        if (browser) {
            cleanupTasks.push(browser.close().catch(e => log(`Browser close error: ${e.message}`, "DEBUG")));
        }
        
        if (pool) {
            cleanupTasks.push(pool.end().catch(e => log(`Pool end error: ${e.message}`, "DEBUG")));
        }
        
        await Promise.allSettled(cleanupTasks);
        
        if (exit) {
            log("Shutdown complete. Exiting process.", "INFO");
            setTimeout(() => process.exit(0), 100);
        }
    }

    // Handle shutdown signals
    process.on('SIGINT', () => {
        log("Received SIGINT signal, initiating graceful shutdown...", "WARN");
        cleanup(true);
    });
    
    process.on('SIGTERM', () => {
        log("Received SIGTERM signal, initiating graceful shutdown...", "WARN");
        cleanup(true);
    });

    // Handle uncaught errors
    process.on('uncaughtException', (error) => {
        log(`Uncaught Exception: ${error.message}`, "CRITICAL");
        log(error.stack, "DEBUG");
        cleanup(true);
    });
    
    process.on('unhandledRejection', (reason, promise) => {
        log(`Unhandled Rejection at: ${promise}. Reason: ${reason}`, "CRITICAL");
        cleanup(true);
    });

    try {
        log("╔══════════════════════════════════════════════════════════╗", "INFO");
        log("║         MONEYVIEW IVR SCRAPER - STARTING                ║", "INFO");
        log("║                    Render Deployment                    ║", "INFO");
        log("╚══════════════════════════════════════════════════════════╝", "INFO");
        
        log(`Database: ${DB_CONFIG.host}/${DB_CONFIG.database}`);
        log(`Login: ${LOGIN_CREDENTIALS.email}`);
        log(`Scrape Interval: ${SCRAPE_INTERVAL_MS/1000} seconds`);
        log(`Running on: ${process.env.RENDER ? 'Render.com' : 'Local'}`);
        
        // Initialize database connection
        pool = await initDB();
        
        // Initialize browser
        browser = await createBrowser();
        page = await browser.newPage();
        
        // Configure page
        await page.setViewport({ width: 1366, height: 768 });
        await page.setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
        
        // Set request timeout
        page.setDefaultTimeout(60000);
        
        // Perform login
        await login(page);
        
        log("╔══════════════════════════════════════════════════════════╗", "SUCCESS");
        log("║          SYSTEM READY - STARTING MAIN LOOP              ║", "SUCCESS");
        log("╚══════════════════════════════════════════════════════════╝", "SUCCESS");
        
        // Main execution loop
        while (true) {
            scrapeCount++;
            const cycleStart = Date.now();
            
            try {
                log(`\n═════════════ Scrape Cycle #${scrapeCount} ═════════════`, "INFO");
                
                const result = await scrape(page, pool);
                
                // Check if scrape was successful
                if (result.errors === 0 || result.inserted > 0) {
                    errorCount = 0; // Reset error count on successful scrape
                    log(`Cycle #${scrapeCount} completed successfully`, "SUCCESS");
                } else {
                    errorCount++;
                    log(`Cycle #${scrapeCount} had issues. Error count: ${errorCount}/${MAX_ERRORS}`, "WARN");
                }
                
                const cycleTime = Date.now() - cycleStart;
                log(`Cycle #${scrapeCount} took ${cycleTime}ms`);
                
                // Check if we need to restart browser due to too many errors
                if (errorCount >= MAX_ERRORS) {
                    log(`Too many consecutive errors (${errorCount}). Restarting browser session...`, "WARN");
                    
                    await cleanup(false);
                    
                    // Reinitialize everything
                    browser = await createBrowser();
                    page = await browser.newPage();
                    await page.setViewport({ width: 1366, height: 768 });
                    await page.setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
                    await login(page);
                    
                    errorCount = 0;
                    log("Browser session restarted successfully", "SUCCESS");
                }
                
                // Wait for next scrape cycle
                const sleepSeconds = SCRAPE_INTERVAL_MS / 1000;
                log(`Sleeping for ${sleepSeconds} seconds until next scrape cycle...`);
                await sleep(SCRAPE_INTERVAL_MS);
                
            } catch (cycleError) {
                errorCount++;
                log(`Cycle #${scrapeCount} failed: ${cycleError.message}`, "ERROR");
                log(`Error count: ${errorCount}/${MAX_ERRORS}`);
                
                if (errorCount >= MAX_ERRORS) {
                    log(`Maximum error threshold reached. Performing full restart...`, "CRITICAL");
                    
                    await cleanup(false);
                    
                    // Full reinitialization
                    browser = await createBrowser();
                    page = await browser.newPage();
                    await page.setViewport({ width: 1366, height: 768 });
                    await page.setUserAgent('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36');
                    await login(page);
                    
                    errorCount = 0;
                    scrapeCount = 0;
                    log("Full system restart completed", "SUCCESS");
                } else {
                    // Wait a bit before retry
                    const retryDelay = Math.min(30000, 5000 * errorCount); // Exponential backoff, max 30s
                    log(`Retrying in ${retryDelay/1000} seconds...`);
                    await sleep(retryDelay);
                }
            }
        }
        
    } catch (fatalError) {
        log(`FATAL STARTUP ERROR: ${fatalError.message}`, "CRITICAL");
        log(fatalError.stack, "DEBUG");
        await cleanup(true);
    }
})();
