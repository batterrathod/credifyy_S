/**
 * Moneyview IVR Scraper – LOOP MODE
 * Runs 24/7 on VPS / Oracle VM / Local / PM2 / Docker
 * ❌ NOT for GitHub Actions
 */

require("dotenv").config();
const puppeteer = require("puppeteer");
const mysql = require("mysql2/promise");

/* ===============================
   CONFIG
================================ */
const SCRAPE_INTERVAL_MS = 20 * 1000; // 20 seconds
const MAX_ERRORS = 5;

const LOGIN_URL = "https://mv-dashboard.switchmyloan.in/login";
const DATA_URL  = "https://mv-dashboard.switchmyloan.in/mv-ivr-logs";

/* ===============================
   ENV VALIDATION
================================ */
function mustEnv(name) {
    if (!process.env[name] || process.env[name].trim() === "") {
        console.error(`❌ MISSING ENV VARIABLE: ${name}`);
        process.exit(1);
    }
    return process.env[name];
}

const EMAIL    = mustEnv("LOGIN_EMAIL");
const PASSWORD = mustEnv("LOGIN_PASSWORD");

/* ===============================
   DATABASE CONFIG
================================ */
const DB_CONFIG = {
    host: mustEnv("DB_HOST"),
    user: mustEnv("DB_USER"),
    password: mustEnv("DB_PASS"),
    database: mustEnv("DB_NAME"),
    port: 3306,
    family: 4,               // 🔥 FIX ::1 localhost issue
    waitForConnections: true,
    connectionLimit: 5
};

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
   HELPERS
================================ */
const sleep = ms => new Promise(r => setTimeout(r, ms));

function log(msg, type = "INFO") {
    console.log(`[${new Date().toISOString()}] [${type}] ${msg}`);
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
   DATABASE INIT
================================ */
async function initDB() {
    log(`Connecting DB → ${DB_CONFIG.host}`);

    const pool = await mysql.createPool(DB_CONFIG);

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
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    `);

    log("Database ready", "SUCCESS");
    return pool;
}

/* ===============================
   BROWSER
================================ */
async function createBrowser() {
    return puppeteer.launch({
        headless: "new",
        args: [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage"
        ]
    });
}

/* ===============================
   LOGIN
================================ */
async function login(page) {
    log("Logging in...");
    await page.goto(LOGIN_URL, { waitUntil: "networkidle0", timeout: 60000 });

    await page.type('input[name="email"]', EMAIL, { delay: 40 });
    await page.type('input[name="password"]', PASSWORD, { delay: 40 });

    await Promise.all([
        page.click('button[type="submit"]'),
        page.waitForNavigation({ waitUntil: "networkidle0", timeout: 60000 })
    ]);

    log("Login successful", "SUCCESS");
}

/* ===============================
   SCRAPE FUNCTION
================================ */
async function scrape(page, pool) {
    log("Scraping IVR logs...");
    await page.goto(DATA_URL, { waitUntil: "networkidle0", timeout: 60000 });
    await page.waitForSelector("tbody tr", { timeout: 30000 });

    const rows = await page.evaluate(() =>
        Array.from(document.querySelectorAll("tbody tr")).map(tr =>
            Array.from(tr.querySelectorAll("td")).map(td =>
                td.textContent.replace(/\s+/g, " ").trim()
            )
        )
    );

    log(`Rows found: ${rows.length}`);

    let inserted = 0;
    let duplicates = 0;

    for (const row of rows) {
        if (row.length < 8) continue;

        try {
            await pool.execute(`
                INSERT INTO ivr_logs
                (sn, full_name, moneyview_msg, phone_number, pan_card, salary, dob_raw, dob, created)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE scrape_timestamp = NOW()
            `, [
                row[IDX_SN],
                row[IDX_NAME],
                row[IDX_MSG],
                row[IDX_NUMBER],
                row[IDX_PAN],
                row[IDX_SALARY],
                row[IDX_DOB],
                parseDate(row[IDX_DOB]),
                row[IDX_CREATED]
            ]);

            inserted++;
        } catch (e) {
            if (e.code === "ER_DUP_ENTRY") duplicates++;
            else log(e.message, "ERROR");
        }
    }

    log(`Inserted: ${inserted} | Duplicates: ${duplicates}`, "SUCCESS");
}

/* ===============================
   MAIN LOOP
================================ */
(async () => {
    let browser = null;
    let page = null;
    let pool = null;
    let errorCount = 0;

    async function cleanup(exit = false) {
        log("Cleaning up...");
        try {
            if (page) await page.close();
            if (browser) await browser.close();
            if (pool) await pool.end();
        } catch (e) {
            log(e.message, "ERROR");
        }
        if (exit) process.exit(0);
    }

    process.on("SIGINT", () => cleanup(true));
    process.on("SIGTERM", () => cleanup(true));

    try {
        log("SCRAPER STARTED (LOOP MODE)");

        pool = await initDB();
        browser = await createBrowser();
        page = await browser.newPage();
        await login(page);

        while (true) {
            try {
                await scrape(page, pool);
                errorCount = 0;
            } catch (err) {
                errorCount++;
                log(`Scrape error (${errorCount}/${MAX_ERRORS}): ${err.message}`, "ERROR");

                if (errorCount >= MAX_ERRORS) {
                    log("Restarting browser...", "WARN");
                    try { await browser.close(); } catch {}
                    browser = await createBrowser();
                    page = await browser.newPage();
                    await login(page);
                    errorCount = 0;
                }
            }

            log(`Sleeping ${SCRAPE_INTERVAL_MS / 1000}s...`);
            await sleep(SCRAPE_INTERVAL_MS);
        }

    } catch (fatal) {
        log(fatal.stack || fatal.message, "CRITICAL");
        await cleanup(true);
    }
})();
