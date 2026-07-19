const express = require("express");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 8080;

app.use(express.json({ limit: "25kb" }));

const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
const FMP_API_KEY = process.env.FMP_API_KEY;
const MASSIVE_API_KEY = process.env.MASSIVE_API_KEY || process.env.POLYGON_API_KEY;
const MASSIVE_BASE_URL = "https://api.massive.com";
const TWELVE_DATA_API_KEY = process.env.TWELVE_DATA_API_KEY;
const FREECRYPTO_API_KEY = process.env.FREECRYPTO_API_KEY;
const FREECRYPTO_BASE_URL = "https://api.freecryptoapi.com/v1";

const priceCache = {};

const TICKERS = [
  "ORNG", "MHRD", "MVDO", "AMZG", "ELPHT", "DATA", "NKLA", "SKYX", "BKSG", "HCC", "ELLY", "PMK", "M", "FMT", "DVS", "WXM",
  "ABMD", "NFKS", "BUM", "DGBE", "REVL", "MNEY", "VKNEE", "BEAR", "NICY", "PPL", "INFO", "OVER", "WBAB", "SMNY", "BC", "RBLX",
  "CHHD", "VSS",
  "MASK", "MNTS", "DSY", "ERNA", "CLDI", "AZI", "DXST", "WCT", "AIXI", "CODX", "GOVX", "CHAI", "CDLX", "DCX", "CLPR"
];

const DISPLAY_TICKER_TO_REAL_TICKER = {
  ORNG: "AAPL",
  MHRD: "MSFT",
  MVDO: "NVDA",
  AMZG: "AMZN",
  ELPHT: "GOOGL",
  DATA: "META",
  NKLA: "TSLA",
  SKYX: "SPCX",
  BKSG: "BRK.B",
  HCC: "AVGO",
  ELLY: "LLY",
  PMK: "JPM",
  M: "V",
  FMT: "WMT",
  DVS: "UNH",
  WXM: "XOM",
  ABMD: "AMD",
  NFKS: "NFLX",
  BUM: "CRM",
  DGBE: "ADBE",
  REVL: "ORCL",
  MNEY: "COST",
  VKNEE: "DIS",
  BEAR: "BA",
  NICY: "NKE",
  PPL: "PYPL",
  INFO: "INTC",
  OVER: "UBER",
  WBAB: "ABNB",
  SMNY: "SBUX",
  BC: "KO",
  RBLX: "RBLX",
  CHHD: "SCHD",
  VSS: "VOO",
  MASK: "MASK",
  MNTS: "MNTS",
  DSY: "DSY",
  ERNA: "ERNA",
  CLDI: "CLDI",
  AZI: "AZI",
  DXST: "DXST",
  WCT: "WCT",
  AIXI: "AIXI",
  CODX: "CODX",
  GOVX: "GOVX",
  CHAI: "CHAI",
  CDLX: "CDLX",
  DCX: "DCX",
  CLPR: "CLPR"
};

const REAL_TICKER_TO_DISPLAY_TICKER = Object.fromEntries(
  Object.entries(DISPLAY_TICKER_TO_REAL_TICKER).map(([displayTicker, realTicker]) => [realTicker, displayTicker])
);

const REAL_STOCK_TICKERS = new Set(
  [...TICKERS, ...Object.values(DISPLAY_TICKER_TO_REAL_TICKER)].map(ticker =>
    String(ticker || "").toUpperCase().replace(/[^A-Z0-9.]/g, "")
  )
);

function normalizeStockTicker(ticker) {
  return String(ticker || "").toUpperCase().replace(/[^A-Z0-9.]/g, "");
}

function getRealTicker(displayTicker) {
  const normalized = normalizeStockTicker(displayTicker);
  return DISPLAY_TICKER_TO_REAL_TICKER[normalized] || normalized;
}

function getDisplayTicker(realTicker) {
  const normalized = normalizeStockTicker(realTicker);
  return REAL_TICKER_TO_DISPLAY_TICKER[normalized] || normalized;
}

function isRealStockTicker(ticker) {
  return REAL_STOCK_TICKERS.has(getRealTicker(ticker));
}

// ============================
// Synthetic stock quotes are disabled
// ============================
// Every configured stock in this game is intended to use real market data.
// Display names can be fake in Roblox, but stock prices/candles must come from real tickers.
// If a real-data provider fails, the server returns an error or stale cache instead of inventing fake prices.
const SYNTHETIC_STOCK_PROFILES = {};

function getSyntheticStockProfile() {
  return null;
}

function applySyntheticStockQuote(ticker) {
  console.warn(`[STOCK] Synthetic quote blocked for ${ticker}. Add/use a real ticker mapping instead.`);
  return null;
}

function applySyntheticProjectStockQuotes() {
  // No-op by design. Do not fabricate stock prices.
}

let wsReady = false;
let wsInstance = null;
let lastWsTradeTime = 0;

const WS_QUIET_THRESHOLD_MS = 60000;

// ============================
// US stock market session status (Eastern Time)
// ============================
function getEasternDateParts(date = new Date()) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23"
  }).formatToParts(date);

  const out = {};

  for (const part of parts) {
    if (part.type !== "literal") out[part.type] = part.value;
  }

  const weekdayMap = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };

  return {
    year: Number(out.year),
    month: Number(out.month),
    day: Number(out.day),
    hour: Number(out.hour),
    min: Number(out.minute),
    sec: Number(out.second),
    wday: weekdayMap[out.weekday] ?? 0
  };
}

function dateKey(year, month, day) {
  return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

function addDaysUtc(year, month, day, delta) {
  const d = new Date(Date.UTC(year, month - 1, day + delta, 12, 0, 0));
  return {
    year: d.getUTCFullYear(),
    month: d.getUTCMonth() + 1,
    day: d.getUTCDate(),
    wday: d.getUTCDay()
  };
}

function weekdayOfDate(year, month, day) {
  return new Date(Date.UTC(year, month - 1, day, 12, 0, 0)).getUTCDay();
}

function nthWeekdayOfMonth(year, month, weekday, nth) {
  const first = weekdayOfDate(year, month, 1);
  const offset = (weekday - first + 7) % 7;
  return 1 + offset + (nth - 1) * 7;
}

function lastWeekdayOfMonth(year, month, weekday) {
  const lastDate = new Date(Date.UTC(year, month, 0, 12, 0, 0)).getUTCDate();
  const lastWday = weekdayOfDate(year, month, lastDate);
  const offset = (lastWday - weekday + 7) % 7;
  return lastDate - offset;
}

function observedFixedHolidayDate(year, month, day) {
  const wday = weekdayOfDate(year, month, day);

  if (wday === 6) return addDaysUtc(year, month, day, -1);
  if (wday === 0) return addDaysUtc(year, month, day, 1);

  return { year, month, day, wday };
}

function easterDate(year) {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31);
  const day = ((h + l - 7 * m + 114) % 31) + 1;

  return { year, month, day };
}

function putHoliday(map, ymd, name) {
  map.set(dateKey(ymd.year, ymd.month, ymd.day), name);
}

function stockMarketHolidayMapForYear(year) {
  const map = new Map();

  putHoliday(map, observedFixedHolidayDate(year, 1, 1), "New Year's Day");
  putHoliday(map, { year, month: 1, day: nthWeekdayOfMonth(year, 1, 1, 3) }, "Martin Luther King Jr. Day");
  putHoliday(map, { year, month: 2, day: nthWeekdayOfMonth(year, 2, 1, 3) }, "Presidents Day");

  const easter = easterDate(year);
  putHoliday(map, addDaysUtc(easter.year, easter.month, easter.day, -2), "Good Friday");

  putHoliday(map, { year, month: 5, day: lastWeekdayOfMonth(year, 5, 1) }, "Memorial Day");
  putHoliday(map, observedFixedHolidayDate(year, 6, 19), "Juneteenth");
  putHoliday(map, observedFixedHolidayDate(year, 7, 4), "Independence Day");
  putHoliday(map, { year, month: 9, day: nthWeekdayOfMonth(year, 9, 1, 1) }, "Labor Day");
  putHoliday(map, { year, month: 11, day: nthWeekdayOfMonth(year, 11, 4, 4) }, "Thanksgiving Day");
  putHoliday(map, observedFixedHolidayDate(year, 12, 25), "Christmas Day");

  return map;
}

function getStockMarketHolidayName(year, month, day) {
  const key = dateKey(year, month, day);

  for (const y of [year - 1, year, year + 1]) {
    const name = stockMarketHolidayMapForYear(y).get(key);
    if (name) return name;
  }

  return null;
}

function isDayBeforeObservedIndependenceDay(year, month, day) {
  const today = dateKey(year, month, day);
  const obs = observedFixedHolidayDate(year, 7, 4);
  const prev = addDaysUtc(obs.year, obs.month, obs.day, -1);
  return today === dateKey(prev.year, prev.month, prev.day);
}

function isChristmasEve(year, month, day) {
  return month === 12 && day === 24;
}

function isDayAfterThanksgiving(year, month, day) {
  const thanks = { year, month: 11, day: nthWeekdayOfMonth(year, 11, 4, 4) };
  const next = addDaysUtc(thanks.year, thanks.month, thanks.day, 1);
  return dateKey(year, month, day) === dateKey(next.year, next.month, next.day);
}

function getEarlyCloseInfo(parts) {
  if (parts.wday === 0 || parts.wday === 6) return null;
  if (getStockMarketHolidayName(parts.year, parts.month, parts.day)) return null;

  if (isDayAfterThanksgiving(parts.year, parts.month, parts.day)) {
    return { name: "Day after Thanksgiving", regularCloseMin: 13 * 60, extendedCloseMin: 17 * 60 };
  }

  if (isChristmasEve(parts.year, parts.month, parts.day)) {
    return { name: "Christmas Eve", regularCloseMin: 13 * 60, extendedCloseMin: 17 * 60 };
  }

  if (isDayBeforeObservedIndependenceDay(parts.year, parts.month, parts.day)) {
    return { name: "Day before Independence Day", regularCloseMin: 13 * 60, extendedCloseMin: 17 * 60 };
  }

  return null;
}

function getMarketSessionStatus(date = new Date()) {
  const parts = getEasternDateParts(date);
  const totalMin = parts.hour * 60 + parts.min;
  const key = dateKey(parts.year, parts.month, parts.day);
  const holidayName = getStockMarketHolidayName(parts.year, parts.month, parts.day);
  const weekend = parts.wday === 0 || parts.wday === 6;
  const earlyClose = getEarlyCloseInfo(parts);
  const regularCloseMin = earlyClose ? earlyClose.regularCloseMin : 16 * 60;
  const extendedCloseMin = earlyClose ? earlyClose.extendedCloseMin : 20 * 60;

  let session = "closed";
  let label = "Market Closed";

  if (holidayName) {
    label = `Market Closed - ${holidayName}`;
  } else if (weekend) {
    label = "Market Closed - Weekend";
  } else if (totalMin >= 4 * 60 && totalMin < 9 * 60 + 30) {
    session = "pre-market";
    label = "Pre-Market";
  } else if (totalMin >= 9 * 60 + 30 && totalMin < regularCloseMin) {
    session = "open";
    label = earlyClose ? "Market Open - Early Close 1:00 PM ET" : "Market Open";
  } else if (totalMin >= regularCloseMin && totalMin < extendedCloseMin) {
    session = "after-hours";
    label = "After Hours";
  }

  return {
    session,
    label,
    date: key,
    timeEt: `${String(parts.hour).padStart(2, "0")}:${String(parts.min).padStart(2, "0")}`,
    isOpen: session === "open",
    isRegular: session === "open",
    isExtended: session === "pre-market" || session === "after-hours",
    isPreMarket: session === "pre-market",
    isAfterHours: session === "after-hours",
    isClosed: session === "closed",
    isWeekend: weekend,
    isHoliday: Boolean(holidayName),
    holidayName,
    earlyClose: Boolean(earlyClose),
    earlyCloseName: earlyClose && earlyClose.name,
    regularCloseEt: earlyClose ? "1:00 PM ET" : "4:00 PM ET",
    extendedCloseEt: earlyClose ? "5:00 PM ET" : "8:00 PM ET"
  };
}

function isRegularMarketHours() {
  return getMarketSessionStatus().session === "open";
}

function isExtendedHours() {
  const session = getMarketSessionStatus().session;
  return session === "pre-market" || session === "after-hours";
}

function isTradingAllowed() {
  const session = getMarketSessionStatus().session;
  return session === "pre-market" || session === "open" || session === "after-hours";
}

// ============================
// Seed from Finnhub
// ============================
async function seedPrevClose() {
  console.log("[SEED] Starting...");

  for (const ticker of TICKERS) {
    if (!isRealStockTicker(ticker)) {
      console.warn(`[SEED] ${ticker} skipped because it is not configured as a real-data stock.`);
      continue;
    }

    try {
      const realTicker = getRealTicker(ticker);
      const res = await fetch(`https://finnhub.io/api/v1/quote?symbol=${realTicker}&token=${FINNHUB_API_KEY}`);
      const data = await res.json();

      if (data.c || data.pc) {
        const existing = priceCache[ticker] || {};
        priceCache[ticker] = {
          price: data.c || data.pc,
          prevClose: data.pc || data.c,
          changePct: data.dp ? data.dp.toFixed(2) : "0.00",
          marketCap: existing.marketCap,
          sharesOutstanding: existing.sharesOutstanding,
          floatShares: existing.floatShares,
          publicFloat: existing.publicFloat
        };

        console.log(`[SEED] ${ticker} = $${priceCache[ticker].price}`);
      }
    } catch (e) {
      console.error(`[SEED] ${ticker} failed`, e.message);
    }
  }
}

// ============================
// Twelve Data Queue
// ============================
const TD_MAX_PER_MINUTE = 7;
const TD_WINDOW_MS = 60 * 1000;
const TD_QUEUE_TIMEOUT_MS = 90 * 1000;
const TD_MAX_QUEUE_LENGTH = 60;

const TD_PRIORITY = {
  candle: 0,
  quote: 1
};

const tdCallTimestamps = [];
const tdQueue = [];

function tdPruneTimestamps(now) {
  const cutoff = now - TD_WINDOW_MS;

  while (tdCallTimestamps.length && tdCallTimestamps[0] < cutoff) {
    tdCallTimestamps.shift();
  }
}

function tdCanSendNow(now) {
  tdPruneTimestamps(now);
  return tdCallTimestamps.length < TD_MAX_PER_MINUTE;
}

function tdProcessQueue() {
  const now = Date.now();

  for (let i = tdQueue.length - 1; i >= 0; i--) {
    if (now - tdQueue[i].enqueuedAt > TD_QUEUE_TIMEOUT_MS) {
      const stale = tdQueue.splice(i, 1)[0];
      stale.reject(new Error("Twelve Data request timed out waiting in queue"));
    }
  }

  if (tdQueue.length === 0) return;

  tdQueue.sort((a, b) => a.priority - b.priority || a.enqueuedAt - b.enqueuedAt);

  while (tdQueue.length > 0 && tdCanSendNow(Date.now())) {
    const job = tdQueue.shift();
    tdCallTimestamps.push(Date.now());

    fetch(job.url)
      .then(r => r.json())
      .then(data => job.resolve(data))
      .catch(err => job.reject(err));
  }
}

setInterval(tdProcessQueue, 200);

function tdRequest(url, priority) {
  return new Promise((resolve, reject) => {
    if (tdQueue.length >= TD_MAX_QUEUE_LENGTH) {
      reject(new Error("Twelve Data request queue is full, try again shortly"));
      return;
    }

    tdQueue.push({
      url,
      resolve,
      reject,
      priority,
      enqueuedAt: Date.now()
    });
  });
}

function tdQueueDepth() {
  return tdQueue.length;
}

function tdCallsInLastMinute() {
  tdPruneTimestamps(Date.now());
  return tdCallTimestamps.length;
}

// ============================
// Twelve Data Polling
// ============================
const BATCH_SIZE = 8;

let twelveDataBatchIndex = 0;
let twelveDataCreditsUsedToday = 0;
let lastCreditReset = new Date().toDateString();

const MAX_CREDITS_PER_DAY = Number(process.env.TWELVE_DATA_MAX_CREDITS_PER_DAY || 50);

// Background Twelve Data quote polling is OFF by default; it was burning daily credits without players opening charts.
// Stock candles use Yahoo Finance first. During regular market hours, Twelve Data is the backup
// if Yahoo fails. Synthetic/fake stock candles are disabled; all stocks use real ticker data.
// Set ENABLE_TWELVE_DATA_CANDLES=true if you want Twelve Data as a fallback outside regular market hours too.
const ENABLE_TWELVE_DATA_POLLING = process.env.ENABLE_TWELVE_DATA_POLLING === "true";
const ENABLE_TWELVE_DATA_CANDLES = process.env.ENABLE_TWELVE_DATA_CANDLES === "true";
const ENABLE_TWELVE_DATA_MARKET_CANDLE_BACKUP = process.env.ENABLE_TWELVE_DATA_MARKET_CANDLE_BACKUP !== "false";

function resetCreditsIfNewDay() {
  const today = new Date().toDateString();

  if (today !== lastCreditReset) {
    twelveDataCreditsUsedToday = 0;
    twelveDataCandleCreditsUsedToday = 0;
    lastCreditReset = today;
    console.log("[12DATA] Credits reset for new day");
  }
}

async function pollTwelveDataBatch() {
  resetCreditsIfNewDay();

  if (twelveDataCreditsUsedToday >= MAX_CREDITS_PER_DAY) {
    console.log("[12DATA] Daily credit limit reached, skipping");
    return;
  }

  const start = twelveDataBatchIndex * BATCH_SIZE;
  const batch = TICKERS.slice(start, start + BATCH_SIZE);

  twelveDataBatchIndex =
    (twelveDataBatchIndex + 1) % Math.ceil(TICKERS.length / BATCH_SIZE);

  if (batch.length === 0) return;

  try {
    const realSymbols = batch.map(getRealTicker);
    const symbols = realSymbols.join(",");
    const url = `https://api.twelvedata.com/quote?symbol=${symbols}&apikey=${TWELVE_DATA_API_KEY}`;
    const data = await tdRequest(url, TD_PRIORITY.quote);

    const results = batch.length === 1 ? { [batch[0]]: data } : data;

    let updated = 0;

    for (const ticker of batch) {
      const realTicker = getRealTicker(ticker);
      const quote = results[realTicker] || results[yahooTickerSymbol(realTicker)] || results[ticker];
      if (!quote || quote.status === "error" || !quote.close) continue;

      const price = parseFloat(quote.close);
      const prevClose =
        parseFloat(quote.previous_close) ||
        (priceCache[ticker] && priceCache[ticker].prevClose) ||
        price;

      const changePct = prevClose
        ? (((price - prevClose) / prevClose) * 100).toFixed(2)
        : "0.00";

      if (!isNaN(price) && price > 0) {
        const existing = priceCache[ticker] || {};
        priceCache[ticker] = {
          price,
          prevClose,
          changePct,
          marketCap: existing.marketCap,
          sharesOutstanding: existing.sharesOutstanding,
          floatShares: existing.floatShares,
          publicFloat: existing.publicFloat
        };

        updated++;
      }
    }

    twelveDataCreditsUsedToday += batch.length;

    console.log(
      `[12DATA] Updated ${updated}/${batch.length} tickers | Credits used today: ${twelveDataCreditsUsedToday}`
    );
  } catch (e) {
    console.error("[12DATA] Batch poll failed", e.message);
  }
}

// ============================
// Yahoo Finance quote polling (regular + pre/post market)
// ============================
// On-demand Yahoo quote refresh is enabled by default. Background polling is OFF by default.
// This keeps Roblox servers fast while preventing every server from constantly burning proxy traffic.
const ENABLE_YAHOO_ON_DEMAND_QUOTES = process.env.ENABLE_YAHOO_ON_DEMAND_QUOTES !== "false";
const ENABLE_YAHOO_QUOTE_POLLING = process.env.ENABLE_YAHOO_QUOTE_POLLING === "true";
const YAHOO_QUOTE_BATCH_SIZE = Number(process.env.YAHOO_QUOTE_BATCH_SIZE || 10);
const YAHOO_QUOTE_POLL_MS = Number(process.env.YAHOO_QUOTE_POLL_MS || 2500);
const PRICE_CACHE_TTL_MS = Number(process.env.PRICE_CACHE_TTL_MS || 15000);
const PRICE_CACHE_MAX_STALE_MS = Number(process.env.PRICE_CACHE_MAX_STALE_MS || 2 * 60 * 1000);
const YAHOO_MISSING_PRICE_FALLBACK_LIMIT = Number(process.env.YAHOO_MISSING_PRICE_FALLBACK_LIMIT || 12);

let yahooQuoteBatchIndex = 0;
let allYahooQuotesFetchedAtMs = 0;
let allYahooQuotesRefreshInFlight = null;
const yahooQuoteInFlight = new Map();

function getYahooDisplayPrice(row) {
  if (!row || typeof row !== "object") return null;

  const state = String(row.marketState || "").toUpperCase();
  const regularPrice = toNumber(row.regularMarketPrice);
  const prePrice = toNumber(row.preMarketPrice);
  const postPrice = toNumber(row.postMarketPrice);
  const regularTime = toNumber(row.regularMarketTime) || 0;
  const preTime = toNumber(row.preMarketTime) || 0;
  const postTime = toNumber(row.postMarketTime) || 0;

  if (state === "PRE" && prePrice && prePrice > 0) {
    return { price: prePrice, source: "Yahoo Finance pre-market", marketState: "PRE", time: preTime || Math.floor(Date.now() / 1000) };
  }

  if ((state === "POST" || state === "POSTPOST") && postPrice && postPrice > 0) {
    return { price: postPrice, source: "Yahoo Finance after-hours", marketState: state, time: postTime || Math.floor(Date.now() / 1000) };
  }

  if (state === "REGULAR" && regularPrice && regularPrice > 0) {
    return { price: regularPrice, source: "Yahoo Finance regular", marketState: "REGULAR", time: regularTime || Math.floor(Date.now() / 1000) };
  }

  if (postPrice && postPrice > 0 && postTime >= Math.max(regularTime, preTime)) {
    return { price: postPrice, source: "Yahoo Finance after-hours", marketState: state || "POST", time: postTime || Math.floor(Date.now() / 1000) };
  }

  if (prePrice && prePrice > 0 && preTime >= Math.max(regularTime, postTime)) {
    return { price: prePrice, source: "Yahoo Finance pre-market", marketState: state || "PRE", time: preTime || Math.floor(Date.now() / 1000) };
  }

  if (regularPrice && regularPrice > 0) {
    return { price: regularPrice, source: "Yahoo Finance regular", marketState: state || "REGULAR", time: regularTime || Math.floor(Date.now() / 1000) };
  }

  return null;
}

function positiveNumberOrNull(value) {
  const n = toNumber(value);
  return n !== null && n > 0 ? n : null;
}

function firstDefinedYahooNumber(...values) {
  for (const value of values) {
    const n = yahooRawNumber(value);
    if (n !== null && n > 0) return n;
  }
  return null;
}

function yahooRawNumber(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value.replace(/,/g, ""));
    return Number.isFinite(parsed) ? parsed : null;
  }
  if (typeof value === "object") {
    if (typeof value.raw === "number" && Number.isFinite(value.raw)) return value.raw;
    if (typeof value.fmt === "string") {
      const parsed = Number(value.fmt.replace(/,/g, ""));
      return Number.isFinite(parsed) ? parsed : null;
    }
    if (typeof value.longFmt === "string") {
      const parsed = Number(value.longFmt.replace(/,/g, ""));
      return Number.isFinite(parsed) ? parsed : null;
    }
  }
  return null;
}

const yahooFloatMetadataCache = {};
const YAHOO_FLOAT_METADATA_TTL_MS = Number(
  process.env.FLOAT_METADATA_TTL_MS ||
  process.env.YAHOO_FLOAT_METADATA_TTL_MS ||
  24 * 60 * 60 * 1000
);
const YAHOO_FLOAT_METADATA_CONCURRENCY = Math.max(1, Math.min(8, Number(process.env.YAHOO_FLOAT_METADATA_CONCURRENCY || 5)));

function getCachedFloatMetadata(displayTicker) {
  const ticker = String(displayTicker || "").toUpperCase();
  const cached = yahooFloatMetadataCache[ticker];
  if (!cached || !cached.fetchedAt) return null;
  if (Date.now() - cached.fetchedAt > YAHOO_FLOAT_METADATA_TTL_MS) return null;
  return cached;
}

function applyFloatMetadataToPriceCache(displayTicker, metadata) {
  const ticker = String(displayTicker || "").toUpperCase();
  if (!ticker || !metadata) return false;

  const existing = priceCache[ticker] || {};
  const derivedSharesFromMarketCap =
    metadata.marketCap && existing.price ? Number(metadata.marketCap) / Number(existing.price) : null;

  const floatShares = firstPositiveNumber(
    metadata.floatShares,
    metadata.publicFloat,
    metadata.sharesOutstanding,
    derivedSharesFromMarketCap,
    existing.floatShares,
    existing.publicFloat
  );
  const sharesOutstanding = firstPositiveNumber(
    metadata.sharesOutstanding,
    derivedSharesFromMarketCap,
    existing.sharesOutstanding,
    floatShares
  );
  const publicFloat = firstPositiveNumber(
    metadata.publicFloat,
    metadata.floatShares,
    floatShares,
    existing.publicFloat
  );
  const marketCap = firstPositiveNumber(metadata.marketCap, existing.marketCap);

  yahooFloatMetadataCache[ticker] = {
    marketCap,
    sharesOutstanding,
    floatShares,
    publicFloat,
    fetchedAt: Date.now()
  };

  if (priceCache[ticker]) {
    priceCache[ticker] = {
      ...existing,
      marketCap,
      sharesOutstanding,
      floatShares,
      publicFloat
    };
  }

  return Boolean(floatShares || sharesOutstanding || marketCap);
}

function fmpTickerSymbol(ticker) {
  return String(ticker || "").toUpperCase().replace(".", "-");
}

function getFmpRecord(data) {
  if (Array.isArray(data)) return data[0] || null;
  if (data && Array.isArray(data.data)) return data.data[0] || null;
  if (data && typeof data === "object") return data;
  return null;
}

function normalizeFmpShareCount(value) {
  const n = firstDefinedYahooNumber(value);
  if (!n || n <= 0) return null;
  return n;
}

function normalizeFmpFreeFloatPercent(value) {
  const n = firstDefinedYahooNumber(value);
  if (!n || n <= 0) return null;
  if (n <= 1) return n * 100;
  if (n <= 100) return n;
  return null;
}

async function fetchFmpFloatMetadata(displayTicker) {
  const ticker = String(displayTicker || "").toUpperCase();
  if (!ticker || !isRealStockTicker(ticker) || !FMP_API_KEY) return false;

  const realTicker = fmpTickerSymbol(getRealTicker(ticker));
  const url =
    `https://financialmodelingprep.com/stable/shares-float` +
    `?symbol=${encodeURIComponent(realTicker)}` +
    `&apikey=${encodeURIComponent(FMP_API_KEY)}`;

  const resp = await fetchJsonWithTimeout(
    url,
    {
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0"
      }
    },
    9000
  );

  if (!resp.ok) {
    throw new Error(resp.data?.Error || resp.data?.error || `FMP shares-float HTTP ${resp.status}`);
  }

  const row = getFmpRecord(resp.data);
  if (!row || typeof row !== "object") {
    throw new Error("FMP shares-float returned no result");
  }

  const outstandingShares = normalizeFmpShareCount(
    row.outstandingShares ??
    row.sharesOutstanding ??
    row.shareOutstanding ??
    row.weightedAverageShsOut ??
    row.weightedAverageShsOutDil
  );

  let floatShares = normalizeFmpShareCount(
    row.floatShares ??
    row.sharesFloat ??
    row.freeFloatShares ??
    row.publicFloat ??
    row.float
  );

  const freeFloatPercent = normalizeFmpFreeFloatPercent(
    row.freeFloat ??
    row.freeFloatPercentage ??
    row.freeFloatPercent
  );

  if (!floatShares && outstandingShares && freeFloatPercent) {
    floatShares = outstandingShares * (freeFloatPercent / 100);
  }

  const marketCap = normalizeFmpShareCount(row.marketCap);

  const metadata = {
    marketCap,
    sharesOutstanding: outstandingShares,
    floatShares: floatShares || outstandingShares,
    publicFloat: floatShares || outstandingShares,
    source: "FMP shares-float"
  };

  return applyFloatMetadataToPriceCache(ticker, metadata);
}

function normalizeFinnhubShareCount(value) {
  const n = firstDefinedYahooNumber(value);
  if (!n || n <= 0) return null;

  // Finnhub stock/metric shareOutstanding and marketCapitalization are commonly returned in millions.
  // If a provider returns a full raw share count instead, keep it as-is.
  if (n < 1000000) return n * 1000000;
  return n;
}

async function fetchFinnhubShareMetadata(displayTicker) {
  const ticker = String(displayTicker || "").toUpperCase();
  if (!ticker || !isRealStockTicker(ticker) || !FINNHUB_API_KEY) return false;

  const realTicker = getRealTicker(ticker);
  const url =
    `https://finnhub.io/api/v1/stock/metric` +
    `?symbol=${encodeURIComponent(realTicker)}` +
    `&metric=all` +
    `&token=${encodeURIComponent(FINNHUB_API_KEY)}`;

  const resp = await fetchJsonWithTimeout(
    url,
    {
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0"
      }
    },
    9000
  );

  if (!resp.ok) {
    throw new Error(resp.data?.error || `Finnhub metric HTTP ${resp.status}`);
  }

  const metric = resp.data && resp.data.metric;
  if (!metric || typeof metric !== "object") {
    throw new Error("Finnhub metric returned no metric object");
  }

  const sharesOutstanding = normalizeFinnhubShareCount(metric.shareOutstanding);
  const possibleFloat = normalizeFinnhubShareCount(
    metric.floatShares || metric.shareFloat || metric.freeFloat || metric.publicFloat
  );
  const marketCapMillions = firstDefinedYahooNumber(metric.marketCapitalization);
  const marketCap = marketCapMillions && marketCapMillions > 0
    ? marketCapMillions * 1000000
    : null;

  const metadata = {
    marketCap,
    sharesOutstanding,
    floatShares: possibleFloat || sharesOutstanding,
    publicFloat: possibleFloat || sharesOutstanding
  };

  return applyFloatMetadataToPriceCache(ticker, metadata);
}

async function fetchYahooFloatMetadata(displayTicker) {
  const ticker = String(displayTicker || "").toUpperCase();
  if (!ticker || !isRealStockTicker(ticker)) return false;

  const cached = getCachedFloatMetadata(ticker);
  if (cached) {
    applyFloatMetadataToPriceCache(ticker, cached);
    return true;
  }

  try {
    if (await fetchFmpFloatMetadata(ticker)) {
      return true;
    }
  } catch (err) {
    console.warn(`[FMP FLOAT] shares-float failed for ${ticker}: ${err.message}`);
  }

  const yahooSymbol = yahooTickerSymbol(getRealTicker(ticker));

  try {
    const url =
      `https://query1.finance.yahoo.com/v10/finance/quoteSummary/${encodeURIComponent(yahooSymbol)}` +
      `?modules=defaultKeyStatistics,price`;

    const resp = await fetchJsonWithTimeout(
      url,
      {
        headers: {
          Accept: "application/json",
          "User-Agent": "Mozilla/5.0"
        }
      },
      9000
    );

    if (!resp.ok) {
      throw new Error(resp.data?.quoteSummary?.error?.description || `Yahoo quoteSummary HTTP ${resp.status}`);
    }

    const result = resp.data?.quoteSummary?.result?.[0];
    if (!result) {
      throw new Error(resp.data?.quoteSummary?.error?.description || "Yahoo quoteSummary returned no result");
    }

    const stats = result.defaultKeyStatistics || {};
    const price = result.price || {};

    const metadata = {
      marketCap: firstDefinedYahooNumber(price.marketCap, stats.marketCap),
      sharesOutstanding: firstDefinedYahooNumber(price.sharesOutstanding, stats.sharesOutstanding),
      floatShares: firstDefinedYahooNumber(stats.floatShares, price.floatShares),
    };
    metadata.publicFloat = firstPositiveNumber(metadata.floatShares, metadata.sharesOutstanding);

    if (applyFloatMetadataToPriceCache(ticker, metadata)) {
      return true;
    }
  } catch (err) {
    console.warn(`[YAHOO FLOAT] quoteSummary failed for ${ticker}: ${err.message}`);
  }

  return fetchFinnhubShareMetadata(ticker);
}

async function enrichYahooFloatMetadataForTickers(tickers) {
  const unique = [...new Set(
    (tickers || [])
      .map(ticker => String(ticker || "").toUpperCase())
      .filter(ticker => ticker && isRealStockTicker(ticker))
  )];

  const stale = unique.filter(ticker => !getCachedFloatMetadata(ticker));
  if (stale.length === 0) {
    unique.forEach(ticker => {
      const cached = getCachedFloatMetadata(ticker);
      if (cached) applyFloatMetadataToPriceCache(ticker, cached);
    });
    return 0;
  }

  let enriched = 0;

  for (let i = 0; i < stale.length; i += YAHOO_FLOAT_METADATA_CONCURRENCY) {
    const batch = stale.slice(i, i + YAHOO_FLOAT_METADATA_CONCURRENCY);
    const results = await Promise.allSettled(batch.map(ticker => fetchYahooFloatMetadata(ticker)));

    results.forEach((result, index) => {
      if (result.status === "fulfilled" && result.value) {
        enriched++;
      } else if (result.status === "rejected") {
        console.warn(`[YAHOO FLOAT] ${batch[index]} unavailable: ${result.reason?.message || result.reason}`);
      }
    });
  }

  if (enriched > 0) {
    console.log(`[YAHOO FLOAT] Enriched ${enriched}/${stale.length} stock floats`);
  }

  return enriched;
}

function applyYahooQuoteToCache(requestedTicker, row) {
  const selected = getYahooDisplayPrice(row);
  if (!selected || !selected.price || selected.price <= 0) return false;

  const ticker = String(requestedTicker || row.symbol || "").toUpperCase().replace("-", ".");
  const existing = priceCache[ticker];
  const prevClose =
    toNumber(row.regularMarketPreviousClose) ||
    toNumber(row.regularMarketPreviousDayClose) ||
    (existing && toNumber(existing.prevClose)) ||
    selected.price;

  const changePct = prevClose
    ? (((selected.price - prevClose) / prevClose) * 100).toFixed(2)
    : "0.00";

  const marketCap = firstPositiveNumber(row.marketCap, existing && existing.marketCap);
  const derivedSharesFromMarketCap = marketCap && selected.price ? marketCap / selected.price : null;
  const sharesOutstanding = firstPositiveNumber(row.sharesOutstanding, derivedSharesFromMarketCap, existing && existing.sharesOutstanding);
  const floatShares = firstPositiveNumber(row.floatShares, row.sharesFloat, row.freeFloat, row.sharesOutstanding, derivedSharesFromMarketCap, existing && existing.floatShares);
  const publicFloat = firstPositiveNumber(floatShares, sharesOutstanding, existing && existing.publicFloat);

  priceCache[ticker] = {
    price: selected.price,
    prevClose,
    changePct,
    source: selected.source,
    marketState: selected.marketState,
    lastUpdated: selected.time || Math.floor(Date.now() / 1000),
    fetchedAt: Date.now(),
    marketCap,
    sharesOutstanding,
    floatShares,
    publicFloat
  };

  return true;
}

async function fetchYahooQuotes(symbols) {
  const requested = symbols
    .map(symbol => String(symbol || "").toUpperCase())
    .filter(Boolean);

  if (requested.length === 0) return 0;

  const yahooSymbols = requested.map(yahooTickerSymbol);
  const quoteFields = [
    "symbol",
    "regularMarketPrice",
    "regularMarketPreviousClose",
    "regularMarketTime",
    "preMarketPrice",
    "preMarketTime",
    "postMarketPrice",
    "postMarketTime",
    "marketState",
    "marketCap",
    "sharesOutstanding",
    "floatShares"
  ].join(",");

  const url =
    `https://query1.finance.yahoo.com/v7/finance/quote` +
    `?symbols=${encodeURIComponent(yahooSymbols.join(","))}` +
    `&fields=${encodeURIComponent(quoteFields)}`;

  const resp = await fetchJsonWithTimeout(
    url,
    {
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0"
      }
    },
    10000
  );

  if (!resp.ok) {
    throw new Error(resp.data?.finance?.error?.description || `Yahoo quote HTTP ${resp.status}`);
  }

  const rows = resp.data?.quoteResponse?.result;
  if (!Array.isArray(rows) || rows.length === 0) return 0;

  const byYahooSymbol = new Map();
  rows.forEach(row => {
    if (row && row.symbol) byYahooSymbol.set(String(row.symbol).toUpperCase(), row);
  });

  let updated = 0;

  requested.forEach((ticker, i) => {
    const row = byYahooSymbol.get(yahooSymbols[i].toUpperCase());
    if (row && applyYahooQuoteToCache(ticker, row)) updated++;
  });

  await enrichYahooFloatMetadataForTickers(requested).catch(err => {
    console.warn(`[YAHOO FLOAT] Batch enrichment failed: ${err.message}`);
  });

  return updated;
}

async function fetchYahooChartQuoteFallback(ticker) {
  ticker = String(ticker || "").toUpperCase();
  if (!ticker || !isRealStockTicker(ticker)) return false;

  const yahooSymbol = yahooTickerSymbol(ticker);
  const url =
    `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(yahooSymbol)}` +
    `?range=5d&interval=1d&includePrePost=false`;

  const resp = await fetchJsonWithTimeout(
    url,
    {
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0"
      }
    },
    12000
  );

  if (!resp.ok) {
    throw new Error(resp.data?.chart?.error?.description || `Yahoo chart quote HTTP ${resp.status}`);
  }

  const chart = resp.data && resp.data.chart;
  const result = chart && Array.isArray(chart.result) && chart.result[0];
  if (!result) {
    throw new Error(chart?.error?.description || "No Yahoo chart quote result returned.");
  }

  const meta = result.meta || {};
  const timestamps = Array.isArray(result.timestamp) ? result.timestamp : [];
  const quote = result.indicators && result.indicators.quote && result.indicators.quote[0];
  const closes = quote && Array.isArray(quote.close) ? quote.close : [];

  let price =
    toNumber(meta.regularMarketPrice) ||
    toNumber(meta.postMarketPrice) ||
    toNumber(meta.preMarketPrice);

  if (!price || price <= 0) {
    for (let i = closes.length - 1; i >= 0; i--) {
      const close = toNumber(closes[i]);
      if (close && close > 0) {
        price = close;
        break;
      }
    }
  }

  if (!price || price <= 0) {
    price = toNumber(meta.previousClose);
  }

  if (!price || price <= 0) {
    throw new Error("Yahoo chart quote returned no usable price.");
  }

  const existing = priceCache[ticker];
  const prevClose =
    toNumber(meta.previousClose) ||
    (existing && toNumber(existing.prevClose)) ||
    price;

  const changePct = prevClose
    ? (((price - prevClose) / prevClose) * 100).toFixed(2)
    : "0.00";

  const lastTimestamp = timestamps.length > 0 ? Number(timestamps[timestamps.length - 1]) : 0;

  priceCache[ticker] = {
    price,
    prevClose,
    changePct,
    source: "Yahoo Finance chart fallback",
    marketState: meta.marketState || "UNKNOWN",
    lastUpdated:
      toNumber(meta.regularMarketTime) ||
      toNumber(meta.postMarketTime) ||
      toNumber(meta.preMarketTime) ||
      lastTimestamp ||
      Math.floor(Date.now() / 1000),
    fetchedAt: Date.now(),
    marketCap: existing && existing.marketCap,
    sharesOutstanding: existing && existing.sharesOutstanding,
    floatShares: existing && existing.floatShares,
    publicFloat: existing && existing.publicFloat
  };

  await enrichYahooFloatMetadataForTickers([ticker]).catch(err => {
    console.warn(`[YAHOO FLOAT] Chart fallback metadata failed for ${ticker}: ${err.message}`);
  });

  return true;
}

async function fillMissingYahooPricesWithChartFallback(limit = YAHOO_MISSING_PRICE_FALLBACK_LIMIT) {
  if (!ENABLE_YAHOO_ON_DEMAND_QUOTES && !ENABLE_YAHOO_QUOTE_POLLING) return 0;

  const missing = TICKERS.filter(ticker => {
    const row = priceCache[ticker];
    return isRealStockTicker(ticker) && (!row || !Number(row.price) || Number(row.price) <= 0);
  }).slice(0, Math.max(0, Number(limit) || 0));

  let updated = 0;

  for (const ticker of missing) {
    try {
      if (await fetchYahooChartQuoteFallback(ticker)) {
        updated++;
      }
    } catch (e) {
      console.warn(`[YAHOO] Chart quote fallback failed for ${ticker}: ${e.message}`);
    }
  }

  if (updated > 0) {
    console.log(`[YAHOO] Chart quote fallback filled ${updated}/${missing.length} missing stock prices`);
  }

  return updated;
}


function yahooQuoteRequestKey(symbols) {
  return symbols
    .map(symbol => String(symbol || "").toUpperCase())
    .filter(Boolean)
    .sort()
    .join(",");
}

function quoteCacheAgeMs(ticker) {
  const data = priceCache[String(ticker || "").toUpperCase()];
  if (!data || !data.fetchedAt) return Infinity;
  return Date.now() - data.fetchedAt;
}

async function fetchYahooQuotesDeduped(symbols) {
  if (!ENABLE_YAHOO_ON_DEMAND_QUOTES && !ENABLE_YAHOO_QUOTE_POLLING) return 0;

  const normalized = symbols
    .map(symbol => String(symbol || "").toUpperCase())
    .filter(Boolean);

  for (const ticker of normalized) {
    if (!isRealStockTicker(ticker)) {
      applySyntheticStockQuote(ticker);
    }
  }

  const requested = normalized.filter(isRealStockTicker);

  if (requested.length === 0) return 0;

  const key = yahooQuoteRequestKey(requested);

  if (yahooQuoteInFlight.has(key)) {
    return yahooQuoteInFlight.get(key);
  }

  const promise = fetchYahooQuotes(requested)
    .finally(() => {
      yahooQuoteInFlight.delete(key);
    });

  yahooQuoteInFlight.set(key, promise);
  return promise;
}

async function refreshAllYahooQuotes() {
  if (!ENABLE_YAHOO_ON_DEMAND_QUOTES && !ENABLE_YAHOO_QUOTE_POLLING) return 0;

  let updated = 0;

  for (let i = 0; i < TICKERS.length; i += YAHOO_QUOTE_BATCH_SIZE) {
    const batch = TICKERS.slice(i, i + YAHOO_QUOTE_BATCH_SIZE);
    updated += await fetchYahooQuotesDeduped(batch);
  }

  updated += await fillMissingYahooPricesWithChartFallback();

  allYahooQuotesFetchedAtMs = Date.now();
  return updated;
}

function triggerAllYahooQuoteRefresh() {
  if (!ENABLE_YAHOO_ON_DEMAND_QUOTES) return null;

  const now = Date.now();

  if (allYahooQuotesRefreshInFlight) return allYahooQuotesRefreshInFlight;
  if (now - allYahooQuotesFetchedAtMs < PRICE_CACHE_TTL_MS) return null;

  allYahooQuotesRefreshInFlight = refreshAllYahooQuotes()
    .catch(err => {
      console.error("[YAHOO] On-demand all quote refresh failed", err.message);
      return 0;
    })
    .finally(() => {
      allYahooQuotesRefreshInFlight = null;
    });

  return allYahooQuotesRefreshInFlight;
}

function triggerYahooQuoteRefresh(symbols) {
  if (!ENABLE_YAHOO_ON_DEMAND_QUOTES) return;

  const staleSymbols = symbols
    .map(symbol => String(symbol || "").toUpperCase())
    .filter(symbol => symbol && quoteCacheAgeMs(symbol) > PRICE_CACHE_TTL_MS);

  if (staleSymbols.length === 0) return;

  fetchYahooQuotesDeduped(staleSymbols).catch(err => {
    console.error("[YAHOO] On-demand quote refresh failed", err.message);
  });
}

async function pollYahooQuoteBatch() {
  if (!ENABLE_YAHOO_QUOTE_POLLING) return;

  const start = yahooQuoteBatchIndex * YAHOO_QUOTE_BATCH_SIZE;
  const batch = TICKERS.slice(start, start + YAHOO_QUOTE_BATCH_SIZE);

  yahooQuoteBatchIndex =
    (yahooQuoteBatchIndex + 1) % Math.ceil(TICKERS.length / YAHOO_QUOTE_BATCH_SIZE);

  if (batch.length === 0) return;

  try {
    const updated = await fetchYahooQuotesDeduped(batch);
    if (updated > 0) {
      console.log(`[YAHOO] Updated ${updated}/${batch.length} stock quotes`);
    }
  } catch (e) {
    console.error("[YAHOO] Quote poll failed", e.message);
  }
}

async function warmYahooQuotes() {
  if (!ENABLE_YAHOO_ON_DEMAND_QUOTES && !ENABLE_YAHOO_QUOTE_POLLING) return;
  await refreshAllYahooQuotes();
}

function startYahooQuotePolling() {
  if (!ENABLE_YAHOO_QUOTE_POLLING) {
    console.log("[YAHOO] Background quote polling disabled. Set ENABLE_YAHOO_QUOTE_POLLING=true to re-enable it.");
    return;
  }

  pollYahooQuoteBatch();
  setInterval(pollYahooQuoteBatch, Math.max(1000, YAHOO_QUOTE_POLL_MS));
}

// ============================
// Finnhub WebSocket
// ============================
function handleTradeMessage(msg) {
  if (msg.type !== "trade" || !Array.isArray(msg.data)) return;

  lastWsTradeTime = Date.now();

  for (const trade of msg.data) {
    const ticker = getDisplayTicker(trade.s);
    const price = trade.p;

    if (!ticker || typeof price !== "number") continue;

    const existing = priceCache[ticker];
    const prevClose = existing && existing.prevClose ? existing.prevClose : price;

    const changePct = prevClose
      ? (((price - prevClose) / prevClose) * 100).toFixed(2)
      : "0.00";

    priceCache[ticker] = {
      price,
      prevClose,
      changePct,
      marketCap: existing && existing.marketCap,
      sharesOutstanding: existing && existing.sharesOutstanding,
      floatShares: existing && existing.floatShares,
      publicFloat: existing && existing.publicFloat
    };
  }
}

function connectFinnhub() {
  const ws = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_API_KEY}`);

  wsInstance = ws;

  ws.on("open", () => {
    console.log("[WS] Connected");
    wsReady = true;

    TICKERS.forEach(ticker => {
      if (!isRealStockTicker(ticker)) return;

      ws.send(JSON.stringify({
        type: "subscribe",
        symbol: getRealTicker(ticker)
      }));
    });
  });

  ws.on("message", raw => {
    try {
      const msg = JSON.parse(raw);
      handleTradeMessage(msg);
    } catch (e) {
      console.error("[WS] Failed to parse message", e.message);
    }
  });

  ws.on("error", err => {
    console.error("[WS] Error", err.message);
  });

  ws.on("close", () => {
    console.log("[WS] Disconnected, reconnecting in 5s...");
    wsReady = false;
    setTimeout(connectFinnhub, 5000);
  });
}

setInterval(() => {
  if (wsInstance && wsInstance.readyState === WebSocket.OPEN) {
    wsInstance.ping();
  }
}, 25000);

// ============================
// Finnhub REST fallback
// ============================
let finnhubTickerIndex = 0;

function startFinnhubRestPolling() {
  setInterval(() => {
    const wsIsQuiet = Date.now() - lastWsTradeTime > WS_QUIET_THRESHOLD_MS;

    if (!wsIsQuiet) return;
    if (!isRegularMarketHours()) return;

    const realTickerPool = TICKERS.filter(isRealStockTicker);
    if (realTickerPool.length === 0) return;

    const ticker = realTickerPool[finnhubTickerIndex % realTickerPool.length];
    const realTicker = getRealTicker(ticker);
    finnhubTickerIndex++;

    (async () => {
      try {
        const res = await fetch(`https://finnhub.io/api/v1/quote?symbol=${realTicker}&token=${FINNHUB_API_KEY}`);
        const data = await res.json();

        if (data.c) {
          const prevClose =
            priceCache[ticker] && priceCache[ticker].prevClose
              ? priceCache[ticker].prevClose
              : data.pc || data.c;

          const changePct = prevClose
            ? (((data.c - prevClose) / prevClose) * 100).toFixed(2)
            : "0.00";

          const existing = priceCache[ticker] || {};
          priceCache[ticker] = {
            price: data.c,
            prevClose,
            changePct,
            marketCap: existing.marketCap,
            sharesOutstanding: existing.sharesOutstanding,
            floatShares: existing.floatShares,
            publicFloat: existing.publicFloat
          };
        }
      } catch (e) {
        console.error(`[POLL] ${ticker} failed`, e.message);
      }
    })();
  }, 1400);
}

function startTwelveDataPolling() {
  setInterval(() => {
    const wsIsQuiet = Date.now() - lastWsTradeTime > WS_QUIET_THRESHOLD_MS;

    if (!wsIsQuiet) return;
    if (!isExtendedHours() && !isRegularMarketHours()) return;

    pollTwelveDataBatch();
  }, 2 * 60 * 1000);
}

// ============================
// Stock candle cache
// ============================
const candleCache = {};

const CANDLE_TTL_MS = {
  "1min": 45 * 1000,
  "5min": 90 * 1000,
  "15min": 2 * 60 * 1000,
  "30min": 4 * 60 * 1000,
  "1h": 8 * 60 * 1000,
  "1day": 30 * 60 * 1000
};

const DEFAULT_CANDLE_TTL_MS = 2 * 60 * 1000;

let twelveDataCandleCreditsUsedToday = 0;
const MAX_TWELVE_DATA_CANDLE_CREDITS_PER_DAY = Number(process.env.TWELVE_DATA_MAX_CANDLE_CREDITS_PER_DAY || 100);

function getCandleTTL(interval) {
  return CANDLE_TTL_MS[interval] || DEFAULT_CANDLE_TTL_MS;
}

function shouldUseTwelveDataCandleBackup(ticker) {
  if (!isRealStockTicker(ticker)) return false;
  if (ENABLE_TWELVE_DATA_CANDLES) return true;
  if (!ENABLE_TWELVE_DATA_MARKET_CANDLE_BACKUP) return false;

  const parts = getEasternDateParts(new Date());
  const totalMin = parts.hour * 60 + parts.min;

  // Also permit the backup after today's session has ended. This matters when an
  // end-of-day Massive plan has not published today's bars yet and Yahoo fails.
  return isStockTradingDateParts(parts) && totalMin >= 4 * 60;
}

function getCachedCandleEntry(ticker, interval) {
  const key = `${ticker}:${interval}`;
  const entry = candleCache[key];

  if (!entry) return null;

  const age = Date.now() - entry.fetchedAt;
  if (age > getCandleTTL(interval)) return null;

  return entry;
}

function getCachedCandles(ticker, interval) {
  const entry = getCachedCandleEntry(ticker, interval);
  return entry ? entry.data : null;
}

function setCachedCandles(ticker, interval, data, source = "Unknown") {
  const key = `${ticker}:${interval}`;
  const cleanedData = repairIsolatedStockWicks(data, interval);

  candleCache[key] = {
    data: cleanedData,
    source,
    fetchedAt: Date.now()
  };
}

function deleteCachedCandles(ticker, interval) {
  delete candleCache[`${ticker}:${interval}`];
}

const RSI_PERIOD = 14;

function attachRsiToCandles(candles, period = RSI_PERIOD) {
  if (!Array.isArray(candles)) return candles;

  const out = candles.map(candle => ({ ...candle }));
  let averageGain = 0;
  let averageLoss = 0;
  let seeded = false;

  for (let i = 1; i < out.length; i++) {
    const previousClose = toNumber(out[i - 1].c ?? out[i - 1].close);
    const currentClose = toNumber(out[i].c ?? out[i].close);

    if (previousClose === null || currentClose === null) {
      continue;
    }

    const change = currentClose - previousClose;
    const gain = Math.max(change, 0);
    const loss = Math.max(-change, 0);

    if (i <= period) {
      averageGain += gain;
      averageLoss += loss;

      if (i === period) {
        averageGain /= period;
        averageLoss /= period;
        seeded = true;
      }
    } else if (seeded) {
      averageGain = ((averageGain * (period - 1)) + gain) / period;
      averageLoss = ((averageLoss * (period - 1)) + loss) / period;
    }

    if (seeded) {
      let rsi;
      if (averageLoss === 0) {
        rsi = 100;
      } else {
        const relativeStrength = averageGain / averageLoss;
        rsi = 100 - (100 / (1 + relativeStrength));
      }

      out[i].rsi = Number(rsi.toFixed(2));
    }
  }

  return out;
}

function withChartIndicators(candles) {
  return attachRsiToCandles(candles, RSI_PERIOD);
}

const stockCandleInFlight = new Map();

async function fetchYahooStockCandlesDeduped(ticker, interval, limit) {
  const key = `${ticker}:${interval}:${limit}`;

  if (stockCandleInFlight.has(key)) {
    return stockCandleInFlight.get(key);
  }

  const promise = fetchYahooStockCandles(ticker, interval, limit)
    .then(candles => {
      setCachedCandles(ticker, interval, candles, "Yahoo Finance");
      return candles;
    })
    .finally(() => {
      stockCandleInFlight.delete(key);
    });

  stockCandleInFlight.set(key, promise);
  return promise;
}


const STOCK_CANDLE_INTERVAL_SECONDS = {
  "1min": 60,
  "5min": 5 * 60,
  "15min": 15 * 60,
  "30min": 30 * 60,
  "1h": 60 * 60,
  "1day": 24 * 60 * 60
};

function stableHashString(value) {
  let hash = 2166136261;
  const str = String(value || "");

  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }

  return hash >>> 0;
}

function deterministicUnit(seed) {
  const x = Math.sin(seed) * 10000;
  return x - Math.floor(x);
}

function roundCandleNumber(n) {
  if (!Number.isFinite(n)) return 0;
  if (Math.abs(n) >= 100) return Number(n.toFixed(2));
  if (Math.abs(n) >= 1) return Number(n.toFixed(4));
  return Number(n.toFixed(6));
}

function formatSyntheticStockCandleTime(ms, interval) {
  const d = new Date(ms);
  const pad = n => String(n).padStart(2, "0");

  const date =
    `${d.getUTCFullYear()}-` +
    `${pad(d.getUTCMonth() + 1)}-` +
    `${pad(d.getUTCDate())}`;

  if (interval === "1day") return date;

  return (
    `${date} ` +
    `${pad(d.getUTCHours())}:` +
    `${pad(d.getUTCMinutes())}:` +
    `${pad(d.getUTCSeconds())}`
  );
}

function parseStockCandleUtcMs(datetimeStr) {
  if (!datetimeStr) return null;

  const m = String(datetimeStr).match(/^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?/);

  if (!m) return null;

  const year = Number(m[1]);
  const month = Number(m[2]);
  const day = Number(m[3]);
  const hour = m[4] !== undefined ? Number(m[4]) : 0;
  const min = m[5] !== undefined ? Number(m[5]) : 0;
  const sec = m[6] !== undefined ? Number(m[6]) : 0;

  const ms = Date.UTC(year, month - 1, day, hour, min, sec);
  return Number.isFinite(ms) ? ms : null;
}

function makeLiveSyntheticCandle(ticker, interval, tMs, open, close, seedOffset = 0) {
  const info = priceCache[ticker];
  const currentPrice = info && toNumber(info.price || info.prevClose);
  const basePrice = currentPrice && currentPrice > 0 ? currentPrice : Math.max(open || close || 1, 0.000001);
  const stepMs = (STOCK_CANDLE_INTERVAL_SECONDS[interval] || 60) * 1000;
  const bucket = Math.floor(tMs / stepMs);
  const hash = stableHashString(`${ticker}:${interval}:${bucket}:${seedOffset}`);

  open = Number(open);
  close = Number(close);

  if (!Number.isFinite(open) || open <= 0) open = basePrice;
  if (!Number.isFinite(close) || close <= 0) close = basePrice;

  const body = Math.abs(close - open);
  const minSpread = Math.max(basePrice * 0.0007, 0.000001);
  const spread = Math.max(body, minSpread);
  const highExtra = spread * (0.25 + deterministicUnit(hash + 3571) * 1.2);
  const lowExtra = spread * (0.25 + deterministicUnit(hash + 5501) * 1.2);

  return {
    t: formatSyntheticStockCandleTime(tMs, interval),
    o: roundCandleNumber(open),
    h: roundCandleNumber(Math.max(open, close) + highExtra),
    l: roundCandleNumber(Math.max(0.000001, Math.min(open, close) - lowExtra)),
    c: roundCandleNumber(close),
    v: Math.round(100000 + deterministicUnit(hash + 1013) * 900000)
  };
}

function patchStockCandlesWithLivePrice(ticker, interval, candles) {
  // Stock charts must display provider OHLCV only.
  // Never create/interpolate candles from the current quote, especially while the market is closed.
  // The old implementation generated fake weekend/overnight price action by filling every missing
  // time bucket up to Date.now(). Keep the function as a compatibility wrapper, but make it a no-op.
  return Array.isArray(candles) ? candles.map(candle => ({ ...candle })) : candles;
}


// ============================
// Massive stock candles (primary accuracy source)
// ============================
// Massive's stock aggregate endpoint is built from qualifying trades and covers
// pre-market, regular hours, and after-hours. For intraday timeframes we request
// the provider's one-minute bars and aggregate them locally with a separate anchor
// for each trading session. That prevents a 30m/1h candle from mixing pre-market
// trades with the 9:30 AM regular open or regular trades with after-hours.
const MASSIVE_CANDLE_CONFIG = {
  "1min": { minutes: 1, lookbackDays: 5 },
  "5min": { minutes: 5, lookbackDays: 7 },
  "15min": { minutes: 15, lookbackDays: 14 },
  "30min": { minutes: 30, lookbackDays: 30 },
  "1h": { minutes: 60, lookbackDays: 45 },
  "1day": { daily: true, lookbackDays: 370 }
};

const massiveStockCandleInFlight = new Map();

function easternPartsToUnixSeconds(parts) {
  if (!parts) return null;

  const year = Number(parts.year);
  const month = Number(parts.month);
  const day = Number(parts.day);
  const hour = Number(parts.hour) || 0;
  const min = Number(parts.min) || 0;
  const sec = Number(parts.sec) || 0;

  if (![year, month, day, hour, min, sec].every(Number.isFinite)) return null;

  // Try EDT and EST, then keep the UTC instant that round-trips to the requested ET clock.
  for (const offsetHours of [4, 5]) {
    const seconds = Math.floor(Date.UTC(year, month - 1, day, hour + offsetHours, min, sec) / 1000);
    const roundTrip = getEasternDateParts(new Date(seconds * 1000));

    if (
      roundTrip.year === year &&
      roundTrip.month === month &&
      roundTrip.day === day &&
      roundTrip.hour === hour &&
      roundTrip.min === min
    ) {
      return seconds;
    }
  }

  return null;
}

function massiveDateRange(lookbackDays) {
  const nowEt = getEasternDateParts(new Date());
  const fromEt = addDaysUtc(nowEt.year, nowEt.month, nowEt.day, -Math.max(1, Number(lookbackDays) || 5));

  return {
    from: dateKey(fromEt.year, fromEt.month, fromEt.day),
    to: dateKey(nowEt.year, nowEt.month, nowEt.day)
  };
}

async function fetchMassiveAggregateRows(realTicker, multiplier, timespan, lookbackDays) {
  if (!MASSIVE_API_KEY) {
    throw new Error("MASSIVE_API_KEY is not set.");
  }

  const range = massiveDateRange(lookbackDays);
  const url =
    `${MASSIVE_BASE_URL}/v2/aggs/ticker/${encodeURIComponent(realTicker)}` +
    `/range/${encodeURIComponent(multiplier)}/${encodeURIComponent(timespan)}` +
    `/${encodeURIComponent(range.from)}/${encodeURIComponent(range.to)}` +
    `?adjusted=true&sort=asc&limit=50000&apiKey=${encodeURIComponent(MASSIVE_API_KEY)}`;

  const resp = await fetchJsonWithTimeout(
    url,
    {
      headers: {
        Accept: "application/json",
        "User-Agent": "Mozilla/5.0"
      }
    },
    15000
  );

  if (!resp.ok) {
    throw new Error(resp.data?.error || resp.data?.message || `Massive aggregate HTTP ${resp.status}`);
  }

  const rows = resp.data && Array.isArray(resp.data.results) ? resp.data.results : [];
  if (rows.length === 0) {
    throw new Error("Massive returned no aggregate bars.");
  }

  return rows;
}

function normalizeMassiveMinuteBars(rows) {
  const normalized = [];
  const seen = new Set();

  for (const row of rows) {
    const milliseconds = Number(row && row.t);
    if (!Number.isFinite(milliseconds) || milliseconds <= 0) continue;

    const seconds = Math.floor(milliseconds / 1000);
    if (seen.has(seconds)) continue;

    const candle = normalizeStockCandleRecord({
      timestampSeconds: seconds,
      interval: "1min",
      open: row.o,
      high: row.h,
      low: row.l,
      close: row.c,
      volume: row.v
    });

    if (!candle) continue;

    // Defensive OHLC validation. Never draw malformed bars.
    if (candle.h < Math.max(candle.o, candle.c) || candle.l > Math.min(candle.o, candle.c)) {
      continue;
    }

    seen.add(seconds);
    normalized.push(candle);
  }

  normalized.sort((a, b) => Number(a.ts || 0) - Number(b.ts || 0));
  return repairIsolatedStockWicks(normalized, "1min");
}

function aggregateMinuteBarsByStockSession(minuteBars, interval, intervalMinutes) {
  if (intervalMinutes <= 1) {
    return minuteBars.map(candle => ({ ...candle }));
  }

  const buckets = new Map();

  for (const candle of minuteBars) {
    const seconds = Number(candle.ts);
    if (!Number.isFinite(seconds) || seconds <= 0) continue;

    const parts = getEasternDateParts(new Date(seconds * 1000));
    const session = normalizeStockSessionNameForServer(candle.session) || classifyStockSessionFromUnixSeconds(seconds);
    if (session !== "pre-market" && session !== "regular" && session !== "after-hours") continue;

    const earlyClose = getEarlyCloseInfo(parts);
    const regularCloseMinute = earlyClose ? earlyClose.regularCloseMin : 16 * 60;
    const sessionStartMinute = session === "pre-market"
      ? 4 * 60
      : (session === "regular" ? 9 * 60 + 30 : regularCloseMinute);

    const totalMinute = parts.hour * 60 + parts.min;
    const offset = totalMinute - sessionStartMinute;
    if (offset < 0) continue;

    const bucketIndex = Math.floor(offset / intervalMinutes);
    const bucketStartMinute = sessionStartMinute + bucketIndex * intervalMinutes;
    const bucketHour = Math.floor(bucketStartMinute / 60);
    const bucketMinute = bucketStartMinute % 60;
    const bucketTimestamp = easternPartsToUnixSeconds({
      year: parts.year,
      month: parts.month,
      day: parts.day,
      hour: bucketHour,
      min: bucketMinute,
      sec: 0
    });

    if (!bucketTimestamp) continue;

    const key = `${parts.year}-${parts.month}-${parts.day}:${session}:${bucketIndex}`;
    let bucket = buckets.get(key);

    if (!bucket) {
      bucket = {
        t: formatSyntheticStockCandleTime(bucketTimestamp * 1000, interval),
        ts: bucketTimestamp,
        session,
        o: candle.o,
        h: candle.h,
        l: candle.l,
        c: candle.c,
        v: Number(candle.v) || 0,
        firstSourceTimestamp: seconds,
        lastSourceTimestamp: seconds
      };
      buckets.set(key, bucket);
    } else {
      // Rows arrive oldest-first, but preserve correctness even if the provider order changes.
      if (seconds < bucket.firstSourceTimestamp) {
        bucket.firstSourceTimestamp = seconds;
        bucket.o = candle.o;
      }
      if (seconds >= bucket.lastSourceTimestamp) {
        bucket.lastSourceTimestamp = seconds;
        bucket.c = candle.c;
      }
      bucket.h = Math.max(bucket.h, candle.h);
      bucket.l = Math.min(bucket.l, candle.l);
      bucket.v += Number(candle.v) || 0;
    }
  }

  const aggregated = [...buckets.values()]
    .sort((a, b) => a.ts - b.ts)
    .map(bucket => ({
      t: bucket.t,
      ts: bucket.ts,
      session: bucket.session,
      o: roundCandleNumber(bucket.o),
      h: roundCandleNumber(bucket.h),
      l: roundCandleNumber(bucket.l),
      c: roundCandleNumber(bucket.c),
      v: Math.round(bucket.v)
    }));

  return repairIsolatedStockWicks(aggregated, interval);
}

function normalizeStockSessionNameForServer(value) {
  const session = String(value || "").toLowerCase().replace(/_/g, "-").replace(/\s+/g, "-");
  if (session === "pre" || session === "premarket" || session === "pre-market") return "pre-market";
  if (session === "regular" || session === "reg" || session === "rth" || session === "open") return "regular";
  if (session === "post" || session === "post-market" || session === "afterhours" || session === "after-hours") return "after-hours";
  if (session === "daily") return "daily";
  if (session === "closed") return "closed";
  return null;
}

function normalizeMassiveDailyBars(rows) {
  const candles = [];
  const seen = new Set();

  for (const row of rows) {
    const milliseconds = Number(row && row.t);
    if (!Number.isFinite(milliseconds) || milliseconds <= 0) continue;

    const seconds = Math.floor(milliseconds / 1000);
    if (seen.has(seconds)) continue;

    const candle = normalizeStockCandleRecord({
      timestampSeconds: seconds,
      interval: "1day",
      open: row.o,
      high: row.h,
      low: row.l,
      close: row.c,
      volume: row.v
    });

    if (!candle) continue;
    if (candle.h < Math.max(candle.o, candle.c) || candle.l > Math.min(candle.o, candle.c)) continue;

    seen.add(seconds);
    candles.push(candle);
  }

  candles.sort((a, b) => Number(a.ts || 0) - Number(b.ts || 0));
  return candles;
}

function candleSeriesLatestTimestamp(candles) {
  if (!Array.isArray(candles) || candles.length === 0) return null;
  for (let i = candles.length - 1; i >= 0; i--) {
    const ts = Number(candles[i] && candles[i].ts);
    if (Number.isFinite(ts) && ts > 0) return ts;
  }
  return null;
}

function isStockTradingDateParts(parts) {
  return Boolean(
    parts &&
    parts.wday !== 0 &&
    parts.wday !== 6 &&
    !getStockMarketHolidayName(parts.year, parts.month, parts.day)
  );
}

function previousStockTradingDate(parts) {
  for (let offset = -1; offset >= -14; offset--) {
    const candidate = addDaysUtc(parts.year, parts.month, parts.day, offset);
    if (isStockTradingDateParts(candidate)) return candidate;
  }
  return addDaysUtc(parts.year, parts.month, parts.day, -1);
}

function expectedLatestStockCandleDateKey(date = new Date()) {
  const parts = getEasternDateParts(date);
  const totalMin = parts.hour * 60 + parts.min;

  // Once pre-market begins, today's chart must contain today's session. Before
  // 4:00 AM ET, or on a weekend/holiday, the newest legitimate date is the
  // previous trading day.
  if (isStockTradingDateParts(parts) && totalMin >= 4 * 60) {
    return dateKey(parts.year, parts.month, parts.day);
  }

  const previous = previousStockTradingDate(parts);
  return dateKey(previous.year, previous.month, previous.day);
}

function candleSeriesLatestEasternDateKey(candles) {
  const latest = candleSeriesLatestTimestamp(candles);
  if (!latest) return null;
  const parts = getEasternDateParts(new Date(latest * 1000));
  return dateKey(parts.year, parts.month, parts.day);
}

function isStockCandleSeriesFreshEnough(candles, interval) {
  const latest = candleSeriesLatestTimestamp(candles);
  if (!latest) return false;

  const ageMs = Date.now() - latest * 1000;
  if (ageMs < 0) return true;

  if (interval === "1day") {
    return ageMs <= 7 * 24 * 60 * 60 * 1000;
  }

  // This is the critical stale-day guard. The old closed-session rule accepted
  // anything less than five days old, so an end-of-day Massive response from
  // yesterday could be treated as current immediately after today's 8 PM close.
  const latestDateKey = candleSeriesLatestEasternDateKey(candles);
  const expectedDateKey = expectedLatestStockCandleDateKey();
  if (!latestDateKey || latestDateKey !== expectedDateKey) return false;

  const status = getMarketSessionStatus();
  if (status.session === "pre-market" || status.session === "open" || status.session === "after-hours") {
    const intervalMs = (STOCK_CANDLE_INTERVAL_SECONDS[interval] || 60) * 1000;
    // Accept real-time or 15-minute-delayed plans, but never an earlier date.
    return ageMs <= Math.max(35 * 60 * 1000, intervalMs * 2 + 20 * 60 * 1000);
  }

  // The expected-date check above handles weekends and holidays. Once today's
  // extended session has ended, same-day candles remain valid until the next
  // session begins.
  return true;
}

function isMassiveCandleSeriesFreshEnough(candles, interval) {
  return isStockCandleSeriesFreshEnough(candles, interval);
}

async function fetchMassiveStockCandles(ticker, interval, limit = 200) {
  const cfg = MASSIVE_CANDLE_CONFIG[interval];
  if (!cfg) throw new Error("Unsupported Massive stock candle interval.");

  const realTicker = getRealTicker(ticker);
  let candles;

  if (cfg.daily) {
    const rows = await fetchMassiveAggregateRows(realTicker, 1, "day", cfg.lookbackDays);
    candles = normalizeMassiveDailyBars(rows);
  } else {
    const rows = await fetchMassiveAggregateRows(realTicker, 1, "minute", cfg.lookbackDays);
    const minuteBars = normalizeMassiveMinuteBars(rows);
    candles = aggregateMinuteBarsByStockSession(minuteBars, interval, cfg.minutes);
  }

  if (!Array.isArray(candles) || candles.length === 0) {
    throw new Error("Massive returned no usable stock candles.");
  }

  return repairIsolatedStockWicks(candles, interval).slice(-limit);
}

async function fetchMassiveStockCandlesDeduped(ticker, interval, limit) {
  const key = `${ticker}:${interval}:${limit}`;
  if (massiveStockCandleInFlight.has(key)) {
    return massiveStockCandleInFlight.get(key);
  }

  const promise = fetchMassiveStockCandles(ticker, interval, limit)
    .finally(() => {
      massiveStockCandleInFlight.delete(key);
    });

  massiveStockCandleInFlight.set(key, promise);
  return promise;
}

const YAHOO_INTERVALS = {
  "1min": { interval: "1m", range: "5d" },
  "5min": { interval: "5m", range: "5d" },
  "15min": { interval: "15m", range: "5d" },
  "30min": { interval: "30m", range: "1mo" },
  "1h": { interval: "60m", range: "1mo" },
  "1day": { interval: "1d", range: "1y" }
};


function classifyStockSessionFromUnixSeconds(timestampSeconds) {
  const seconds = Number(timestampSeconds);
  if (!Number.isFinite(seconds) || seconds <= 0) return "closed";

  const parts = getEasternDateParts(new Date(seconds * 1000));
  const weekend = parts.wday === 0 || parts.wday === 6;
  const holiday = getStockMarketHolidayName(parts.year, parts.month, parts.day);

  if (weekend || holiday) return "closed";

  const totalMin = parts.hour * 60 + parts.min;
  const earlyClose = getEarlyCloseInfo(parts);
  const regularCloseMin = earlyClose ? earlyClose.regularCloseMin : 16 * 60;
  const extendedCloseMin = 20 * 60;

  if (totalMin >= 4 * 60 && totalMin < 9 * 60 + 30) return "pre-market";
  if (totalMin >= 9 * 60 + 30 && totalMin < regularCloseMin) return "regular";
  if (totalMin >= regularCloseMin && totalMin < extendedCloseMin) return "after-hours";

  return "closed";
}

function parseExchangeLocalDateTime(value) {
  const match = String(value || "").match(
    /^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?/
  );

  if (!match) return null;

  return {
    year: Number(match[1]),
    month: Number(match[2]),
    day: Number(match[3]),
    hour: Number(match[4] || 0),
    min: Number(match[5] || 0),
    sec: Number(match[6] || 0),
    hasTime: match[4] !== undefined
  };
}

function easternLocalDateTimeToUnixSeconds(value) {
  const parts = parseExchangeLocalDateTime(value);
  if (!parts || !parts.hasTime) return null;

  // Twelve Data stock timestamps are exchange-local. Try both US Eastern offsets
  // and keep the UTC timestamp that converts back to the exact supplied ET clock.
  for (const offsetHours of [4, 5]) {
    const seconds = Math.floor(
      Date.UTC(
        parts.year,
        parts.month - 1,
        parts.day,
        parts.hour + offsetHours,
        parts.min,
        parts.sec
      ) / 1000
    );

    const roundTrip = getEasternDateParts(new Date(seconds * 1000));
    if (
      roundTrip.year === parts.year &&
      roundTrip.month === parts.month &&
      roundTrip.day === parts.day &&
      roundTrip.hour === parts.hour &&
      roundTrip.min === parts.min
    ) {
      return seconds;
    }
  }

  return null;
}

function normalizeStockCandleRecord({
  timestampSeconds,
  datetime,
  interval,
  open,
  high,
  low,
  close,
  volume
}) {
  const o = Number(open);
  const h = Number(high);
  const l = Number(low);
  const c = Number(close);
  const v = Number(volume) || 0;

  if (
    !Number.isFinite(o) ||
    !Number.isFinite(h) ||
    !Number.isFinite(l) ||
    !Number.isFinite(c) ||
    o <= 0 ||
    h <= 0 ||
    l <= 0 ||
    c <= 0
  ) {
    return null;
  }

  let seconds = Number(timestampSeconds);
  if (!Number.isFinite(seconds) || seconds <= 0) {
    seconds = easternLocalDateTimeToUnixSeconds(datetime);
  }

  const isDaily = interval === "1day";
  const session = isDaily
    ? "daily"
    : (seconds ? classifyStockSessionFromUnixSeconds(seconds) : "closed");

  // Intraday stock charts only contain the actual 4:00 AM-8:00 PM ET trading session.
  // This also strips any stray weekend/overnight rows returned by a provider.
  if (!isDaily && session === "closed") return null;

  return {
    t: seconds
      ? formatSyntheticStockCandleTime(seconds * 1000, interval)
      : String(datetime || ""),
    ts: seconds || undefined,
    session,
    o: roundCandleNumber(o),
    h: roundCandleNumber(h),
    l: roundCandleNumber(l),
    c: roundCandleNumber(c),
    v
  };
}


// Repairs isolated provider bad ticks without smoothing legitimate price action.
// A bar is changed only when its body and nearby bars remain tightly grouped,
// one wick is far outside that local market, and the bar does not carry an
// unusually large volume spike. This specifically removes false vertical wicks
// occasionally present in aggregate feeds while preserving normal candles.
function medianFinite(values) {
  const sorted = values
    .map(Number)
    .filter(Number.isFinite)
    .sort((a, b) => a - b);

  if (sorted.length === 0) return 0;
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1
    ? sorted[middle]
    : (sorted[middle - 1] + sorted[middle]) / 2;
}

const STOCK_BAD_WICK_MIN_PERCENT = {
  "1min": 0.0035,
  "5min": 0.005,
  "15min": 0.008,
  "30min": 0.012,
  "1h": 0.02
};

function repairIsolatedStockWicks(candles, interval) {
  if (!Array.isArray(candles) || candles.length < 3 || interval === "1day") {
    return candles;
  }

  const intervalSeconds = STOCK_CANDLE_INTERVAL_SECONDS[interval] || 60;
  const source = candles.map(candle => ({ ...candle }));
  const repaired = source.map(candle => ({ ...candle }));

  const intervalConfig = {
    "1min": { window: 7, wickPct: 0.0035, bodyPct: 0.009, maxCluster: 8 },
    "5min": { window: 6, wickPct: 0.0050, bodyPct: 0.012, maxCluster: 6 },
    "15min": { window: 5, wickPct: 0.0080, bodyPct: 0.018, maxCluster: 4 },
    "30min": { window: 4, wickPct: 0.0120, bodyPct: 0.025, maxCluster: 3 },
    "1h": { window: 4, wickPct: 0.0200, bodyPct: 0.040, maxCluster: 2 }
  }[interval] || { window: 6, wickPct: 0.005, bodyPct: 0.012, maxCluster: 6 };

  const validBar = candle => {
    if (!candle) return false;
    const values = [candle.o, candle.h, candle.l, candle.c].map(Number);
    return values.every(value => Number.isFinite(value) && value > 0);
  };

  const bodyCenter = candle => (Number(candle.o) + Number(candle.c)) / 2;
  const bodyRange = candle => Math.abs(Number(candle.c) - Number(candle.o));

  const sameSession = (left, right) => {
    if (!left || !right) return false;
    if (left.session && right.session && left.session !== right.session) return false;
    return true;
  };

  const nearbyInTime = (left, right, maxIntervals = 10) => {
    const leftTs = Number(left && left.ts);
    const rightTs = Number(right && right.ts);
    if (!Number.isFinite(leftTs) || !Number.isFinite(rightTs)) return true;
    return Math.abs(leftTs - rightTs) <= intervalSeconds * maxIntervals;
  };

  // Build the local market from candle BODIES only. Bad provider wicks can occur
  // in several consecutive rows; using neighboring highs/lows made the previous
  // filter treat those bad rows as the new "normal" and leave them untouched.
  const localStats = index => {
    const current = source[index];
    if (!validBar(current)) return null;

    const centers = [];
    const bodyRanges = [];
    const startIndex = Math.max(0, index - intervalConfig.window);
    const endIndex = Math.min(source.length - 1, index + intervalConfig.window);

    for (let j = startIndex; j <= endIndex; j++) {
      if (j === index) continue;
      const candidate = source[j];
      if (!validBar(candidate)) continue;
      if (!sameSession(current, candidate)) continue;
      if (!nearbyInTime(current, candidate, intervalConfig.window + 2)) continue;

      centers.push(bodyCenter(candidate));
      bodyRanges.push(bodyRange(candidate));
    }

    if (centers.length < 3) return null;

    const center = medianFinite(centers);
    const deviations = centers.map(value => Math.abs(value - center));
    const mad = medianFinite(deviations);
    const medianBody = medianFinite(bodyRanges);
    const scale = Math.max(
      medianBody,
      mad * 1.4826,
      center * 0.00025,
      0.02
    );

    return { center, scale };
  };

  // Pass 1: clamp abnormal high/low prints even when several adjacent candles
  // have the same corrupted wick. Candle bodies must still be near the local
  // market, so real sustained moves and gaps are preserved.
  for (let index = 0; index < source.length; index++) {
    const candle = source[index];
    const stats = localStats(index);
    if (!validBar(candle) || !stats) continue;

    const open = Number(candle.o);
    const close = Number(candle.c);
    const high = Number(candle.h);
    const low = Number(candle.l);
    const bodyHigh = Math.max(open, close);
    const bodyLow = Math.min(open, close);
    const bodyMid = (open + close) / 2;

    const bodyTolerance = Math.max(
      stats.center * intervalConfig.bodyPct,
      stats.scale * 12,
      0.25
    );
    const bodyLooksLocal =
      Math.abs(bodyMid - stats.center) <= bodyTolerance &&
      Math.abs(open - stats.center) <= bodyTolerance * 1.35 &&
      Math.abs(close - stats.center) <= bodyTolerance * 1.35;

    if (!bodyLooksLocal) continue;

    const wickTrigger = Math.max(
      stats.center * intervalConfig.wickPct,
      stats.scale * 10,
      0.20
    );
    const wickToKeep = Math.max(
      stats.center * 0.0015,
      stats.scale * 4,
      0.06
    );

    let changed = false;
    let safeHigh = high;
    let safeLow = low;

    if ((high - bodyHigh) > wickTrigger) {
      safeHigh = bodyHigh + wickToKeep;
      changed = true;
    }
    if ((bodyLow - low) > wickTrigger) {
      safeLow = bodyLow - wickToKeep;
      changed = true;
    }

    if (changed) {
      repaired[index].h = roundCandleNumber(Math.max(bodyHigh, safeHigh));
      repaired[index].l = roundCandleNumber(Math.min(bodyLow, safeLow));
      repaired[index].badTickRepaired = true;
      repaired[index].badTickRepairType = "cluster-wick-clamp";
    }
  }

  // Pass 2: detect short runs whose candle bodies themselves are far away from
  // the local market. A run is bridged only when stable bars on BOTH sides return
  // to nearly the same price. This catches malformed multi-row drops without
  // flattening a genuine move that continues.
  const bodyOutlier = new Array(source.length).fill(false);
  const statsByIndex = new Array(source.length).fill(null);

  for (let index = 0; index < source.length; index++) {
    const candle = source[index];
    const stats = localStats(index);
    statsByIndex[index] = stats;
    if (!validBar(candle) || !stats) continue;

    const mid = bodyCenter(candle);
    const threshold = Math.max(
      stats.center * intervalConfig.bodyPct,
      stats.scale * 12,
      0.30
    );
    bodyOutlier[index] = Math.abs(mid - stats.center) > threshold;
  }

  let runStart = 0;
  while (runStart < bodyOutlier.length) {
    if (!bodyOutlier[runStart]) {
      runStart++;
      continue;
    }

    let runEnd = runStart;
    while (runEnd + 1 < bodyOutlier.length && bodyOutlier[runEnd + 1]) {
      runEnd++;
    }

    const runLength = runEnd - runStart + 1;
    const previousIndex = runStart - 1;
    const nextIndex = runEnd + 1;
    const previous = source[previousIndex];
    const next = source[nextIndex];
    const stats = statsByIndex[runStart] || statsByIndex[runEnd];

    if (
      runLength <= intervalConfig.maxCluster &&
      validBar(previous) &&
      validBar(next) &&
      stats &&
      sameSession(previous, next) &&
      nearbyInTime(previous, next, runLength + 3)
    ) {
      const leftPrice = Number(previous.c);
      const rightPrice = Number(next.o);
      const bridgeTolerance = Math.max(
        stats.center * 0.006,
        stats.scale * 14,
        0.40
      );

      if (
        Math.abs(leftPrice - rightPrice) <= bridgeTolerance &&
        Math.abs(leftPrice - stats.center) <= bridgeTolerance * 1.5 &&
        Math.abs(rightPrice - stats.center) <= bridgeTolerance * 1.5
      ) {
        let priorClose = leftPrice;
        for (let index = runStart; index <= runEnd; index++) {
          const progress = (index - runStart + 1) / (runLength + 1);
          const bridgedClose = leftPrice + ((rightPrice - leftPrice) * progress);
          const bridgedOpen = priorClose;
          const wickToKeep = Math.max(
            stats.center * 0.0015,
            stats.scale * 4,
            0.06
          );
          const bodyHigh = Math.max(bridgedOpen, bridgedClose);
          const bodyLow = Math.min(bridgedOpen, bridgedClose);

          repaired[index].o = roundCandleNumber(bridgedOpen);
          repaired[index].c = roundCandleNumber(bridgedClose);
          repaired[index].h = roundCandleNumber(bodyHigh + wickToKeep);
          repaired[index].l = roundCandleNumber(bodyLow - wickToKeep);
          repaired[index].badTickRepaired = true;
          repaired[index].badTickRepairType = "cluster-body-bridge";
          priorClose = bridgedClose;
        }
      }
    }

    runStart = runEnd + 1;
  }

  return repaired;
}

async function fetchYahooStockCandles(ticker, interval, limit = 200) {
  const cfg = YAHOO_INTERVALS[interval];

  if (!cfg) {
    throw new Error("Unsupported Yahoo stock candle interval.");
  }

  const yahooSymbol = yahooTickerSymbol(ticker);
  const hosts = [
    "query1.finance.yahoo.com",
    "query2.finance.yahoo.com"
  ];
  const errors = [];

  for (const host of hosts) {
    const url =
      `https://${host}/v8/finance/chart/${encodeURIComponent(yahooSymbol)}` +
      `?range=${encodeURIComponent(cfg.range)}` +
      `&interval=${encodeURIComponent(cfg.interval)}` +
      `&includePrePost=true` +
      `&events=div%2Csplits` +
      `&lang=en-US&region=US&_=${Date.now()}`;

    try {
      const resp = await fetchJsonWithTimeout(
        url,
        {
          headers: {
            Accept: "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            Pragma: "no-cache",
            Referer: "https://finance.yahoo.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131 Safari/537.36"
          }
        },
        15000
      );

      if (!resp.ok) {
        throw new Error(resp.data?.chart?.error?.description || `Yahoo ${host} HTTP ${resp.status}`);
      }

      const chart = resp.data && resp.data.chart;
      const result = chart && Array.isArray(chart.result) && chart.result[0];

      if (!result) {
        throw new Error(chart?.error?.description || `No Yahoo chart result returned by ${host}.`);
      }

      const timestamps = Array.isArray(result.timestamp) ? result.timestamp : [];
      const quote = result.indicators && result.indicators.quote && result.indicators.quote[0];

      if (!quote || timestamps.length === 0) {
        throw new Error(`Yahoo ${host} chart result missing quote data.`);
      }

      const candles = [];

      for (let i = 0; i < timestamps.length; i++) {
        const o = toNumber(quote.open && quote.open[i]);
        const h = toNumber(quote.high && quote.high[i]);
        const l = toNumber(quote.low && quote.low[i]);
        const c = toNumber(quote.close && quote.close[i]);
        const v = toNumber(quote.volume && quote.volume[i]) || 0;

        if (o === null || h === null || l === null || c === null) continue;
        if (o <= 0 || h <= 0 || l <= 0 || c <= 0) continue;

        const candle = normalizeStockCandleRecord({
          timestampSeconds: Number(timestamps[i]),
          interval,
          open: o,
          high: h,
          low: l,
          close: c,
          volume: v
        });

        if (candle) candles.push(candle);
      }

      if (candles.length === 0) {
        throw new Error(`Yahoo ${host} returned no usable candle data.`);
      }

      return repairIsolatedStockWicks(candles, interval).slice(-limit);
    } catch (error) {
      errors.push(`${host}: ${error && error.message || String(error)}`);
    }
  }

  throw new Error(`Yahoo candle hosts failed. ${errors.join(" | ")}`);
}

function generateSyntheticStockCandles(ticker, interval, limit = 200) {
  return {
    error: "Synthetic stock candles are disabled. This game uses real ticker data only."
  };
}

// ============================
// Crypto live prices
// ============================
const CRYPTO_SYMBOLS = ["BTC", "ETH", "SOL", "DOGE", "LTC"];

const CRYPTO_NAMES = {
  BTC: "Bitcoin",
  ETH: "Ethereum",
  SOL: "Solana",
  DOGE: "Dogecoin",
  LTC: "Litecoin"
};

const COINGECKO_IDS = {
  BTC: "bitcoin",
  ETH: "ethereum",
  SOL: "solana",
  DOGE: "dogecoin",
  LTC: "litecoin"
};

const CRYPTO_LIVE_PAIRS = {
  BTC: "BTCUSDT",
  ETH: "ETHUSDT",
  SOL: "SOLUSDT",
  DOGE: "DOGEUSDT",
  LTC: "LTCUSDT"
};

const CRYPTO_PAIR_TO_SYMBOL = Object.fromEntries(
  Object.entries(CRYPTO_LIVE_PAIRS).map(([symbol, pair]) => [pair, symbol])
);

const cryptoPriceCache = {};
let cryptoCacheFetchedAt = 0;

// Binance ticker streams publish at 1000ms. REST is only a startup/watchdog fallback.
const CRYPTO_CACHE_TTL_MS = 1500;
const CRYPTO_STREAM_STALE_MS = 5500;
const CRYPTO_REST_WATCHDOG_MS = 10000;

let cryptoTickerWs = null;
let cryptoTickerWsSourceIndex = 0;
let cryptoTickerWsLastMessageAt = 0;
let cryptoTickerWsReconnectTimer = null;
let cryptoTickerWatchdog = null;
let cryptoRestRefreshInProgress = false;

const CRYPTO_STREAM_SOURCES = [
  {
    name: "Binance global WebSocket",
    base: "wss://stream.binance.com:9443"
  },
  {
    name: "Binance.US WebSocket",
    base: "wss://stream.binance.us:9443"
  }
];

function toNumber(value) {
  if (value === null || value === undefined) return null;

  if (typeof value === "string") {
    const cleaned = value.replace(/[$,%\s,]/g, "");
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : null;
  }

  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function normalizeSymbol(symbol) {
  return String(symbol || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

function parseMaybeJson(value) {
  if (typeof value !== "string") return value;

  const trimmed = value.trim();

  if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) return value;

  try {
    return JSON.parse(trimmed);
  } catch (_) {
    return value;
  }
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 15000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const resp = await fetch(url, {
      ...options,
      signal: controller.signal
    });

    const text = await resp.text();

    let data = null;

    try {
      data = JSON.parse(text);
      data = parseMaybeJson(data);
    } catch (_) {
      data = {
        rawText: text
      };
    }

    return {
      ok: resp.ok,
      status: resp.status,
      data,
      rawText: text
    };
  } finally {
    clearTimeout(timer);
  }
}

const PRICE_KEYS = [
  "price",
  "price_usd",
  "priceUsd",
  "usd_price",
  "usdPrice",
  "current_price",
  "currentPrice",
  "last_price",
  "lastPrice",
  "last",
  "close",
  "rate",
  "value",
  "usd",
  "USD"
];

const CHANGE_KEYS = [
  "change_24h",
  "change24h",
  "change_24H",
  "percent_change_24h",
  "percentChange24h",
  "price_change_percentage_24h",
  "changePct",
  "change_pct",
  "change",
  "priceChangePercent"
];

const MARKET_CAP_KEYS = [
  "market_cap",
  "marketCap",
  "market_cap_usd",
  "marketCapUsd",
  "usd_market_cap"
];

const VOLUME_KEYS = [
  "volume",
  "volume_24h",
  "volume24h",
  "total_volume",
  "usd_24h_vol",
  "quoteVolume"
];

function pickNumber(obj, keys) {
  if (!obj || typeof obj !== "object") return null;

  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      const n = toNumber(obj[key]);
      if (n !== null) return n;
    }
  }

  return null;
}

function collectObjectsDeep(value, out = [], depth = 0, keyHint = null) {
  value = parseMaybeJson(value);

  if (!value || depth > 8) return out;

  if (Array.isArray(value)) {
    for (const item of value) {
      collectObjectsDeep(item, out, depth + 1, keyHint);
    }

    return out;
  }

  if (typeof value !== "object") return out;

  if (keyHint && !value._keyHint) {
    try {
      value._keyHint = keyHint;
    } catch (_) {}
  }

  out.push(value);

  for (const [key, child] of Object.entries(value)) {
    const childKeyHint = CRYPTO_SYMBOLS.includes(normalizeSymbol(key))
      ? normalizeSymbol(key)
      : keyHint;

    collectObjectsDeep(child, out, depth + 1, childKeyHint);
  }

  return out;
}

function findNumberDeep(value, keys, depth = 0) {
  value = parseMaybeJson(value);

  if (!value || depth > 7) return null;
  if (typeof value !== "object") return null;

  const direct = pickNumber(value, keys);
  if (direct !== null) return direct;

  for (const child of Object.values(value)) {
    const nested = findNumberDeep(child, keys, depth + 1);
    if (nested !== null) return nested;
  }

  return null;
}

function objectMentionsSymbol(obj, symbol) {
  if (!obj || typeof obj !== "object") return false;

  const wanted = normalizeSymbol(symbol);

  const keys = [
    "symbol",
    "ticker",
    "code",
    "asset",
    "base",
    "coin"
  ];

  for (const key of keys) {
    if (normalizeSymbol(obj[key]) === wanted) return true;
  }

  if (normalizeSymbol(obj._keyHint) === wanted) return true;

  return false;
}

function findBestObjectForSymbol(payload, symbol, allowGenericFallback = true) {
  const objects = collectObjectsDeep(payload);
  const wanted = normalizeSymbol(symbol);

  for (const obj of objects) {
    if (objectMentionsSymbol(obj, wanted) && findNumberDeep(obj, PRICE_KEYS) !== null) {
      return obj;
    }
  }

  for (const obj of objects) {
    if (normalizeSymbol(obj._keyHint) === wanted && findNumberDeep(obj, PRICE_KEYS) !== null) {
      return obj;
    }
  }

  if (allowGenericFallback) {
    for (const obj of objects) {
      if (findNumberDeep(obj, PRICE_KEYS) !== null) {
        return obj;
      }
    }
  }

  return null;
}

function normalizeCryptoPriceFromPayload(payload, symbol, sourceName, allowGenericFallback = true) {
  const normalizedSymbol = normalizeSymbol(symbol);

  const obj = findBestObjectForSymbol(
    payload,
    normalizedSymbol,
    allowGenericFallback
  );

  if (!obj) return null;

  const price = findNumberDeep(obj, PRICE_KEYS);

  if (price === null || price <= 0) return null;

  const nowMs = Date.now();
  return {
    symbol: normalizedSymbol,
    name:
      obj.name ||
      obj.fullName ||
      obj.full_name ||
      CRYPTO_NAMES[normalizedSymbol] ||
      normalizedSymbol,
    price,
    change24h: findNumberDeep(obj, CHANGE_KEYS),
    marketCap: findNumberDeep(obj, MARKET_CAP_KEYS),
    volume24h: findNumberDeep(obj, VOLUME_KEYS),
    lastUpdated: Math.floor(nowMs / 1000),
    lastUpdatedMs: nowMs,
    receivedAtMs: nowMs,
    source: sourceName
  };
}

function normalizeManyCryptoPrices(payload, symbols, sourceName) {
  const out = {};

  for (const symbol of symbols) {
    const info = normalizeCryptoPriceFromPayload(
      payload,
      symbol,
      sourceName,
      false
    );

    if (info) out[symbol] = info;
  }

  return out;
}

function normalizeBinanceTickerRow(row, sourceName) {
  if (!row || typeof row !== "object") return null;

  const pair = normalizeSymbol(row.s || row.symbol);
  const symbol = CRYPTO_PAIR_TO_SYMBOL[pair];
  if (!symbol) return null;

  const price = toNumber(row.c ?? row.lastPrice ?? row.price);
  if (price === null || price <= 0) return null;

  const eventMs = toNumber(row.E ?? row.closeTime) || Date.now();
  const existing = cryptoPriceCache[symbol] || {};

  return {
    symbol,
    name: CRYPTO_NAMES[symbol] || symbol,
    price,
    change24h: toNumber(row.P ?? row.priceChangePercent) ?? existing.change24h ?? null,
    marketCap: existing.marketCap ?? null,
    volume24h: toNumber(row.q ?? row.quoteVolume) ?? existing.volume24h ?? null,
    lastUpdated: Math.floor(eventMs / 1000),
    lastUpdatedMs: eventMs,
    receivedAtMs: Date.now(),
    source: sourceName
  };
}

function applyBinanceTickerRow(row, sourceName) {
  const info = normalizeBinanceTickerRow(row, sourceName);
  if (!info) return false;

  cryptoPriceCache[info.symbol] = info;
  cryptoCacheFetchedAt = Date.now();
  return true;
}

function cryptoStreamUrl(source) {
  const streams = Object.values(CRYPTO_LIVE_PAIRS)
    .map(pair => `${pair.toLowerCase()}@ticker`)
    .join("/");
  return `${source.base}/stream?streams=${streams}`;
}

function scheduleCryptoTickerReconnect(delayMs = 2000) {
  if (cryptoTickerWsReconnectTimer) return;

  cryptoTickerWsReconnectTimer = setTimeout(() => {
    cryptoTickerWsReconnectTimer = null;
    cryptoTickerWsSourceIndex = (cryptoTickerWsSourceIndex + 1) % CRYPTO_STREAM_SOURCES.length;
    connectCryptoTickerStream();
  }, delayMs);
}

function connectCryptoTickerStream() {
  const source = CRYPTO_STREAM_SOURCES[cryptoTickerWsSourceIndex];

  if (cryptoTickerWs) {
    try {
      cryptoTickerWs.removeAllListeners();
      cryptoTickerWs.terminate();
    } catch (_) {}
    cryptoTickerWs = null;
  }

  const ws = new WebSocket(cryptoStreamUrl(source));
  cryptoTickerWs = ws;

  ws.on("open", () => {
    cryptoTickerWsLastMessageAt = Date.now();
    console.log(`[CRYPTO WS] Connected to ${source.name}`);
  });

  ws.on("message", raw => {
    try {
      const parsed = JSON.parse(String(raw));
      const row = parsed && parsed.data ? parsed.data : parsed;
      if (applyBinanceTickerRow(row, source.name)) {
        cryptoTickerWsLastMessageAt = Date.now();
      }
    } catch (e) {
      console.warn(`[CRYPTO WS] Bad message from ${source.name}: ${e.message}`);
    }
  });

  ws.on("error", err => {
    console.warn(`[CRYPTO WS] ${source.name} error: ${err.message}`);
  });

  ws.on("close", () => {
    if (cryptoTickerWs === ws) cryptoTickerWs = null;
    console.warn(`[CRYPTO WS] Disconnected from ${source.name}; switching/retrying.`);
    scheduleCryptoTickerReconnect(2000);
  });
}

function isCryptoStreamHealthy() {
  return Boolean(
    cryptoTickerWs &&
    cryptoTickerWs.readyState === WebSocket.OPEN &&
    Date.now() - cryptoTickerWsLastMessageAt <= CRYPTO_STREAM_STALE_MS
  );
}

async function fetchBinanceRestPrices(symbols, baseUrl, sourceName) {
  const requested = symbols.filter(symbol => CRYPTO_LIVE_PAIRS[symbol]);
  if (requested.length === 0) return { prices: {} };

  const pairs = requested.map(symbol => CRYPTO_LIVE_PAIRS[symbol]);
  const prices = {};
  let lastError = null;

  const parseRows = data => {
    const rows = Array.isArray(data) ? data : [data];
    for (const row of rows) {
      const info = normalizeBinanceTickerRow(row, sourceName);
      if (info && requested.includes(info.symbol)) {
        prices[info.symbol] = info;
      }
    }
  };

  try {
    const url = `${baseUrl}/api/v3/ticker/24hr?symbols=${encodeURIComponent(JSON.stringify(pairs))}`;
    const resp = await fetchJsonWithTimeout(url, { headers: { Accept: "application/json" } }, 7000);
    if (resp.ok) {
      parseRows(resp.data);
    } else {
      lastError = `${sourceName} HTTP ${resp.status}`;
    }
  } catch (e) {
    lastError = e.message || `${sourceName} batch request failed.`;
  }

  // Some regional deployments do not accept the symbols array. Fill any missing
  // assets with inexpensive individual requests rather than returning a partial set.
  for (const symbol of requested) {
    if (prices[symbol]) continue;
    const pair = CRYPTO_LIVE_PAIRS[symbol];
    try {
      const url = `${baseUrl}/api/v3/ticker/24hr?symbol=${encodeURIComponent(pair)}`;
      const resp = await fetchJsonWithTimeout(url, { headers: { Accept: "application/json" } }, 5000);
      if (resp.ok) {
        parseRows(resp.data);
      } else {
        lastError = `${sourceName} ${symbol} HTTP ${resp.status}`;
      }
    } catch (e) {
      lastError = e.message || `${sourceName} ${symbol} request failed.`;
    }
  }

  return { prices, error: lastError };
}

async function fetchFreeCryptoAPI(symbols) {
  if (!FREECRYPTO_API_KEY) {
    return {
      prices: {},
      error: "FREECRYPTO_API_KEY is not set on the proxy server."
    };
  }

  const headers = {
    Authorization: `Bearer ${FREECRYPTO_API_KEY}`,
    Accept: "application/json"
  };

  const prices = {};

  let lastError = null;

  try {
    const joined = symbols.join("+");
    const batchUrl = `${FREECRYPTO_BASE_URL}/getData?symbol=${encodeURIComponent(joined)}`;
    const batch = await fetchJsonWithTimeout(batchUrl, { headers });

    if (batch.ok) {
      Object.assign(
        prices,
        normalizeManyCryptoPrices(batch.data, symbols, "FreeCryptoAPI fallback")
      );
    } else {
      lastError =
        batch.data?.message ||
        batch.data?.error ||
        `FreeCryptoAPI HTTP ${batch.status}`;
    }
  } catch (e) {
    lastError = e.message || "FreeCryptoAPI batch request failed.";
  }

  for (const symbol of symbols) {
    if (prices[symbol]) continue;

    try {
      const singleUrl = `${FREECRYPTO_BASE_URL}/getData?symbol=${encodeURIComponent(symbol)}`;
      const single = await fetchJsonWithTimeout(singleUrl, { headers });

      if (!single.ok) {
        lastError =
          single.data?.message ||
          single.data?.error ||
          `FreeCryptoAPI HTTP ${single.status}`;

        continue;
      }

      const info = normalizeCryptoPriceFromPayload(
        single.data,
        symbol,
        "FreeCryptoAPI fallback"
      );

      if (info) prices[symbol] = info;
    } catch (e) {
      lastError = e.message || `FreeCryptoAPI ${symbol} request failed.`;
    }
  }

  return {
    prices,
    error: lastError
  };
}

async function fetchCoinGeckoFallback(symbols) {
  const ids = symbols
    .map(symbol => COINGECKO_IDS[symbol])
    .filter(Boolean);

  if (ids.length === 0) {
    return {
      prices: {},
      error: "No CoinGecko IDs available."
    };
  }

  const url =
    `https://api.coingecko.com/api/v3/simple/price` +
    `?ids=${encodeURIComponent(ids.join(","))}` +
    `&vs_currencies=usd` +
    `&include_market_cap=true` +
    `&include_24hr_vol=true` +
    `&include_24hr_change=true`;

  try {
    const resp = await fetchJsonWithTimeout(url, {
      headers: {
        Accept: "application/json"
      }
    });

    if (!resp.ok) {
      return {
        prices: {},
        error: resp.data?.error || `CoinGecko HTTP ${resp.status}`
      };
    }

    const byId = resp.data || {};
    const prices = {};

    for (const symbol of symbols) {
      const id = COINGECKO_IDS[symbol];
      const row = byId[id];

      if (!row) continue;

      const price = toNumber(row.usd);

      if (price === null || price <= 0) continue;

      const nowMs = Date.now();
      prices[symbol] = {
        symbol,
        name: CRYPTO_NAMES[symbol] || symbol,
        price,
        change24h: toNumber(row.usd_24h_change),
        marketCap: toNumber(row.usd_market_cap),
        volume24h: toNumber(row.usd_24h_vol),
        lastUpdated: Math.floor(nowMs / 1000),
        lastUpdatedMs: nowMs,
        receivedAtMs: nowMs,
        source: "CoinGecko fallback"
      };
    }

    return {
      prices
    };
  } catch (e) {
    return {
      prices: {},
      error: e.message || "CoinGecko fallback request failed."
    };
  }
}

function buildCryptoPriceResponse(requestedSymbols, extra = {}) {
  const prices = {};
  let newestReceivedAt = 0;

  for (const symbol of requestedSymbols) {
    const info = cryptoPriceCache[symbol];
    if (!info) continue;
    prices[symbol] = info;
    newestReceivedAt = Math.max(newestReceivedAt, Number(info.receivedAtMs) || 0);
  }

  return {
    prices,
    cached: extra.cached === true,
    streamHealthy: isCryptoStreamHealthy(),
    serverTimeMs: Date.now(),
    newestPriceAgeMs: newestReceivedAt > 0 ? Math.max(0, Date.now() - newestReceivedAt) : null,
    stale: extra.stale === true,
    providerError: extra.providerError || null
  };
}

async function fetchCryptoPrices(symbols = CRYPTO_SYMBOLS, options = {}) {
  const requestedSymbols = [...new Set(
    symbols
      .map(normalizeSymbol)
      .filter(symbol => CRYPTO_SYMBOLS.includes(symbol))
  )];

  const wanted = requestedSymbols.length > 0 ? requestedSymbols : CRYPTO_SYMBOLS;
  const now = Date.now();

  const cacheHasAllRequested = wanted.every(symbol => {
    const info = cryptoPriceCache[symbol];
    return info && toNumber(info.price) > 0;
  });

  const cacheIsFresh = wanted.every(symbol => {
    const info = cryptoPriceCache[symbol];
    const receivedAt = info && Number(info.receivedAtMs);
    return receivedAt > 0 && now - receivedAt <= CRYPTO_STREAM_STALE_MS;
  });

  if (!options.forceRest && cacheHasAllRequested && cacheIsFresh) {
    return buildCryptoPriceResponse(wanted, { cached: true });
  }

  if (cryptoRestRefreshInProgress && cacheHasAllRequested) {
    return buildCryptoPriceResponse(wanted, {
      cached: true,
      stale: !cacheIsFresh
    });
  }

  cryptoRestRefreshInProgress = true;
  let providerError = null;

  try {
    let missing = wanted.filter(symbol => !cryptoPriceCache[symbol] || !cacheIsFresh);

    const globalResult = await fetchBinanceRestPrices(
      missing,
      "https://api.binance.com",
      "Binance global REST"
    );
    Object.assign(cryptoPriceCache, globalResult.prices);
    providerError = globalResult.error || providerError;

    missing = wanted.filter(symbol => !cryptoPriceCache[symbol] || (Date.now() - (cryptoPriceCache[symbol].receivedAtMs || 0)) > CRYPTO_STREAM_STALE_MS);
    if (missing.length > 0) {
      const usResult = await fetchBinanceRestPrices(
        missing,
        "https://api.binance.us",
        "Binance.US REST"
      );
      Object.assign(cryptoPriceCache, usResult.prices);
      providerError = usResult.error || providerError;
    }

    missing = wanted.filter(symbol => !cryptoPriceCache[symbol]);
    if (missing.length > 0) {
      const freeCrypto = await fetchFreeCryptoAPI(missing);
      Object.assign(cryptoPriceCache, freeCrypto.prices);
      providerError = freeCrypto.error || providerError;
    }

    missing = wanted.filter(symbol => !cryptoPriceCache[symbol]);
    if (missing.length > 0) {
      const fallback = await fetchCoinGeckoFallback(missing);
      Object.assign(cryptoPriceCache, fallback.prices);
      providerError = fallback.error || providerError;
    }

    cryptoCacheFetchedAt = Date.now();
  } finally {
    cryptoRestRefreshInProgress = false;
  }

  const finalHasAny = wanted.some(symbol => cryptoPriceCache[symbol]);
  if (!finalHasAny) {
    return {
      prices: {},
      error: providerError || "No usable real crypto prices were returned.",
      streamHealthy: isCryptoStreamHealthy(),
      serverTimeMs: Date.now(),
      stale: true
    };
  }

  const stale = wanted.some(symbol => {
    const info = cryptoPriceCache[symbol];
    return !info || Date.now() - (Number(info.receivedAtMs) || 0) > CRYPTO_REST_WATCHDOG_MS;
  });

  return buildCryptoPriceResponse(wanted, {
    cached: false,
    stale,
    providerError
  });
}

function startCryptoPriceWatchdog() {
  if (cryptoTickerWatchdog) return;

  cryptoTickerWatchdog = setInterval(() => {
    const stale = Date.now() - cryptoTickerWsLastMessageAt > CRYPTO_STREAM_STALE_MS;

    if (!cryptoTickerWs || cryptoTickerWs.readyState !== WebSocket.OPEN || stale) {
      if (cryptoTickerWs) {
        try {
          cryptoTickerWs.terminate();
        } catch (_) {}
      } else {
        scheduleCryptoTickerReconnect(500);
      }
    }

    if (stale) {
      fetchCryptoPrices(CRYPTO_SYMBOLS, { forceRest: true }).catch(err => {
        console.warn(`[CRYPTO REST] Watchdog refresh failed: ${err.message}`);
      });
    }
  }, 3000);
}


// ============================
// Crypto candles - free Binance public klines
// ============================
const BINANCE_GLOBAL_BASE_URL = "https://api.binance.com";
const BINANCE_US_BASE_URL = "https://api.binance.us";

const CRYPTO_BINANCE_SYMBOLS = {
  BTC: "BTCUSDT",
  ETH: "ETHUSDT",
  SOL: "SOLUSDT",
  DOGE: "DOGEUSDT",
  LTC: "LTCUSDT"
};

const CRYPTO_CANDLE_INTERVALS = {
  "1m": "1m",
  "5m": "5m",
  "15m": "15m",
  "1h": "1h",
  "1d": "1d"
};

const cryptoCandleCache = {};

const CRYPTO_CANDLE_TTL_MS = {
  "1m": 5000,
  "5m": 10000,
  "15m": 20000,
  "1h": 60000,
  "1d": 5 * 60 * 1000
};

function getCryptoCandleTTL(interval) {
  return CRYPTO_CANDLE_TTL_MS[interval] || 15000;
}

function getCachedCryptoCandles(symbol, interval) {
  const key = `${symbol}:${interval}`;
  const entry = cryptoCandleCache[key];

  if (!entry) return null;

  if (Date.now() - entry.fetchedAt > getCryptoCandleTTL(interval)) {
    return null;
  }

  return entry;
}

function setCachedCryptoCandles(symbol, interval, data, source) {
  const key = `${symbol}:${interval}`;

  cryptoCandleCache[key] = {
    data,
    source,
    fetchedAt: Date.now()
  };
}

function formatUtcCandleTime(ms) {
  const d = new Date(ms);
  const pad = n => String(n).padStart(2, "0");

  return (
    `${d.getUTCFullYear()}-` +
    `${pad(d.getUTCMonth() + 1)}-` +
    `${pad(d.getUTCDate())} ` +
    `${pad(d.getUTCHours())}:` +
    `${pad(d.getUTCMinutes())}:` +
    `${pad(d.getUTCSeconds())}`
  );
}

function normalizeBinanceKlines(raw) {
  if (!Array.isArray(raw)) return [];

  return raw
    .filter(k => Array.isArray(k) && k.length >= 6)
    .map(k => ({
      t: formatUtcCandleTime(Number(k[0])),
      o: Number(k[1]),
      h: Number(k[2]),
      l: Number(k[3]),
      c: Number(k[4]),
      v: Number(k[5])
    }))
    .filter(c => {
      return (
        Number.isFinite(c.o) &&
        Number.isFinite(c.h) &&
        Number.isFinite(c.l) &&
        Number.isFinite(c.c)
      );
    });
}

async function fetchBinanceKlines(baseUrl, pair, interval, limit) {
  const url =
    `${baseUrl}/api/v3/klines` +
    `?symbol=${encodeURIComponent(pair)}` +
    `&interval=${encodeURIComponent(interval)}` +
    `&limit=${encodeURIComponent(limit)}`;

  const resp = await fetchJsonWithTimeout(
    url,
    {
      headers: {
        Accept: "application/json"
      }
    },
    12000
  );

  if (!resp.ok) {
    const msg =
      resp.data?.msg ||
      resp.data?.message ||
      resp.data?.error ||
      `HTTP ${resp.status}`;

    throw new Error(msg);
  }

  const candles = normalizeBinanceKlines(resp.data);

  if (candles.length === 0) {
    throw new Error("No usable Binance kline data returned.");
  }

  return candles;
}

async function fetchCryptoCandles(symbol, interval) {
  symbol = normalizeSymbol(symbol);
  interval = String(interval || "1m").toLowerCase();

  if (!CRYPTO_SYMBOLS.includes(symbol)) {
    return {
      error: "Unsupported crypto symbol."
    };
  }

  if (!CRYPTO_CANDLE_INTERVALS[interval]) {
    return {
      error: "Unsupported crypto interval. Use 1m, 5m, 15m, 1h, or 1d."
    };
  }

  const cached = getCachedCryptoCandles(symbol, interval);

  if (cached) {
    return {
      symbol,
      interval,
      candles: withChartIndicators(cached.data),
      cached: true,
      source: cached.source
    };
  }

  const pair = CRYPTO_BINANCE_SYMBOLS[symbol];
  const binanceInterval = CRYPTO_CANDLE_INTERVALS[interval];
  const limit = 200;

  let lastError = null;

  try {
    const candles = await fetchBinanceKlines(
      BINANCE_GLOBAL_BASE_URL,
      pair,
      binanceInterval,
      limit
    );

    setCachedCryptoCandles(symbol, interval, candles, "Binance global");

    return {
      symbol,
      interval,
      pair,
      candles: withChartIndicators(candles),
      cached: false,
      source: "Binance global"
    };
  } catch (e) {
    lastError = `Binance global: ${e.message}`;
  }

  try {
    const candles = await fetchBinanceKlines(
      BINANCE_US_BASE_URL,
      pair,
      binanceInterval,
      limit
    );

    setCachedCryptoCandles(symbol, interval, candles, "Binance.US");

    return {
      symbol,
      interval,
      pair,
      candles: withChartIndicators(candles),
      cached: false,
      source: "Binance.US"
    };
  } catch (e) {
    lastError = `${lastError}; Binance.US: ${e.message}`;
  }

  return {
    error: lastError || "Crypto candle request failed."
  };
}


// ============================
// Roblox group role sync
// ============================
// Roblox cannot safely hold your Open Cloud API key inside replicated client code.
// This endpoint lets a Roblox SERVER script send the player's in-game role here,
// then this Node backend updates the player's group role through Roblox Open Cloud.
//
// Required environment variables on your deployment host:
// ROBLOX_OPEN_CLOUD_API_KEY = Open Cloud API key with group:write permission
// GROUP_SYNC_SECRET = random shared secret; must match the Roblox ServerScript
//
// Optional:
// ROBLOX_GROUP_ID = 15696460
// GROUP_ROLE_INTERN_NAME = Intern Trader
// GROUP_ROLE_ROOKIE_NAME = Rookie Trader
// GROUP_ROLE_INTERMEDIATE_NAME = Intermediate Trader
// GROUP_ROLE_DAY_TRADER_NAME = Day Trader
// GROUP_ROLE_PROFESSIONAL_NAME = Professional Trader
// GROUP_ROLE_TOP_TRADER_NAME = Top Trader at GCF
// GROUP_ROLE_TRADING_MANAGER_NAME = Trading Firm Manager at GCF
// GROUP_ROLE_CFO_NAME = Chief Financial Officer at GCF
// GROUP_ROLE_CEO_NAME = Chief Executive Officer at GCF
// GROUP_ROLE_INDEPENDENT_NAME = Independent Professional Trader
// GROUP_ROLE_MULTI_MILLIONAIRE_NAME = Multi-Millionaire Trader

const ROBLOX_GROUP_ID = String(process.env.ROBLOX_GROUP_ID || "15696460");
const ROBLOX_OPEN_CLOUD_API_KEY = process.env.ROBLOX_OPEN_CLOUD_API_KEY || "";
const GROUP_SYNC_SECRET = process.env.GROUP_SYNC_SECRET || "";

function roleCandidates(envValue, ...fallbackNames) {
  const names = [];
  if (envValue) names.push(String(envValue));
  fallbackNames.forEach(name => names.push(String(name || "")));
  return [...new Set(names.map(name => name.trim()).filter(Boolean))];
}

const GAME_ROLE_TO_GROUP_ROLE_CANDIDATES = {
  "Intern": roleCandidates(process.env.GROUP_ROLE_INTERN_NAME, "Intern Trader", "Intern"),
  "Intern Trader": roleCandidates(process.env.GROUP_ROLE_INTERN_NAME, "Intern Trader", "Intern"),
  "Rookie Trader": roleCandidates(process.env.GROUP_ROLE_ROOKIE_NAME, "Rookie Trader"),
  "Intermediate Trader": roleCandidates(process.env.GROUP_ROLE_INTERMEDIATE_NAME, "Intermediate Trader"),
  "Day Trader": roleCandidates(process.env.GROUP_ROLE_DAY_TRADER_NAME, "Day Trader"),
  "Professional Trader": roleCandidates(process.env.GROUP_ROLE_PROFESSIONAL_NAME, "Professional Trader"),
  "Top Trader at GCF": roleCandidates(process.env.GROUP_ROLE_TOP_TRADER_NAME, "Top Trader at GCF", "Top Trader"),
  "Trading Firm Manager at GCF": roleCandidates(process.env.GROUP_ROLE_TRADING_MANAGER_NAME, "Trading Firm Manager at GCF", "Trading Firm Manager"),
  "Chief Financial Officer at GCF": roleCandidates(process.env.GROUP_ROLE_CFO_NAME, "Chief Financial Officer at GCF", "Chief Financial Officer"),
  "Chief Executive Officer at GCF": roleCandidates(process.env.GROUP_ROLE_CEO_NAME, "Chief Executive Officer at GCF", "Chief Executive Officer"),
  "Chief Executive Officer": roleCandidates(process.env.GROUP_ROLE_CEO_NAME, "Chief Executive Officer at GCF", "Chief Executive Officer"),
  "Independent Professional Trader": roleCandidates(process.env.GROUP_ROLE_INDEPENDENT_NAME, "Independent Professional Trader"),
  "Multi-Millionaire Trader": roleCandidates(process.env.GROUP_ROLE_MULTI_MILLIONAIRE_NAME, "Multi-Millionaire Trader")
};

const GAME_ROLE_TO_GROUP_ROLE_NAME = Object.fromEntries(
  Object.entries(GAME_ROLE_TO_GROUP_ROLE_CANDIDATES).map(([role, candidates]) => [role, candidates[0] || role])
);

const GAME_ROLE_ALIASES = {
  "intern": "Intern Trader",
  "intern trader": "Intern Trader",
  "rookie": "Rookie Trader",
  "rookie trader": "Rookie Trader",
  "intermediate": "Intermediate Trader",
  "intermediate trader": "Intermediate Trader",
  "day trader": "Day Trader",
  "professional trader": "Professional Trader",
  "top trader": "Top Trader at GCF",
  "top trader at gcf": "Top Trader at GCF",
  "trading firm manager": "Trading Firm Manager at GCF",
  "trading firm manager at gcf": "Trading Firm Manager at GCF",
  "chief financial officer": "Chief Financial Officer at GCF",
  "chief financial officer at gcf": "Chief Financial Officer at GCF",
  "chief executive officer": "Chief Executive Officer at GCF",
  "chief executive officer at gcf": "Chief Executive Officer at GCF",
  "independent professional trader": "Independent Professional Trader",
  "multi millionaire trader": "Multi-Millionaire Trader",
  "multi-millionaire trader": "Multi-Millionaire Trader"
};

let cachedGroupRolesByDisplayName = null;
let cachedGroupRolesFetchedAtMs = 0;
const GROUP_ROLE_CACHE_TTL_MS = 10 * 60 * 1000;

function normalizeGameRole(role) {
  role = String(role || "").trim();
  role = role.replace(/^\d+\.\s*/, "");
  if (Object.prototype.hasOwnProperty.call(GAME_ROLE_TO_GROUP_ROLE_CANDIDATES, role)) return role;

  const normalizedKey = role.toLowerCase().replace(/\s+/g, " ").trim();
  const alias = GAME_ROLE_ALIASES[normalizedKey];
  return alias && Object.prototype.hasOwnProperty.call(GAME_ROLE_TO_GROUP_ROLE_CANDIDATES, alias) ? alias : "";
}

function getGroupRoleCandidates(gameRole) {
  return GAME_ROLE_TO_GROUP_ROLE_CANDIDATES[gameRole] || [];
}

function getGroupRoleDisplayName(role) {
  return String(role && (role.displayName || role.name || role.roleName || "") || "").trim();
}

function getGroupRoleResource(role) {
  const path = String(role && (role.path || "") || "").trim();
  if (/^groups\/\d+\/roles\/\d+$/.test(path)) {
    return path;
  }

  const name = String(role && (role.name || "") || "").trim();
  if (/^groups\/\d+\/roles\/\d+$/.test(name)) {
    return name;
  }

  const id = String(role && (role.id || role.roleId || "") || "").trim();
  if (/^\d+$/.test(id)) {
    return `groups/${ROBLOX_GROUP_ID}/roles/${id}`;
  }

  return "";
}

function listMembershipsFromPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return [];
  }

  if (Array.isArray(payload.groupMemberships)) return payload.groupMemberships;
  if (Array.isArray(payload.memberships)) return payload.memberships;
  if (Array.isArray(payload.data)) return payload.data;
  return [];
}

function getMembershipResource(membership) {
  const path = String(membership && (membership.path || "") || "").trim();
  if (/^groups\/\d+\/memberships\/[A-Za-z0-9_-]+$/.test(path)) {
    return path;
  }

  const name = String(membership && (membership.name || "") || "").trim();
  if (/^groups\/\d+\/memberships\/[A-Za-z0-9_-]+$/.test(name)) {
    return name;
  }

  const id = String(
    membership && (
      membership.id ||
      membership.membershipId ||
      membership.groupMembershipId ||
      ""
    ) || ""
  ).trim();

  if (id) {
    return `groups/${ROBLOX_GROUP_ID}/memberships/${id}`;
  }

  return "";
}

function getMembershipUserId(membership) {
  if (!membership || typeof membership !== "object") return 0;

  const direct =
    membership.userId ||
    membership.memberUserId ||
    (membership.user && (membership.user.id || membership.user.userId));

  const directNumber = Number(direct);
  if (Number.isInteger(directNumber) && directNumber > 0) {
    return directNumber;
  }

  const userResource = String(
    membership.user ||
    membership.userPath ||
    membership.userName ||
    (membership.member && membership.member.user) ||
    (membership.user && (membership.user.path || membership.user.name)) ||
    ""
  );

  const match = userResource.match(/users\/(\d+)/);
  return match ? Number(match[1]) : 0;
}

function membershipHasRole(membership, groupRoleResource) {
  if (!membership || !groupRoleResource) return false;

  const singleRole = String(membership.role || "").trim();
  if (singleRole === groupRoleResource) return true;

  const topRole = String(membership.topRole || membership.highestRole || "").trim();
  if (topRole === groupRoleResource) return true;

  const roles = []
    .concat(Array.isArray(membership.roles) ? membership.roles : [])
    .concat(Array.isArray(membership.assignedRoles) ? membership.assignedRoles : []);

  return roles.some(role => {
    const resource = typeof role === "string"
      ? role
      : String(role && (role.path || role.name || role.role || "") || "");
    return resource === groupRoleResource;
  });
}

function membershipBelongsToGroup(membership, groupId) {
  const wanted = String(groupId || "");
  const resource = getMembershipResource(membership);
  if (resource.startsWith(`groups/${wanted}/memberships/`)) return true;

  const groupValue = membership && (
    membership.group ||
    membership.groupPath ||
    membership.groupName ||
    (membership.group && (membership.group.path || membership.group.name || membership.group.id))
  );

  const groupText = String(groupValue || "");
  if (groupText === `groups/${wanted}`) return true;
  if (groupText === wanted) return true;
  if (groupText.includes(`groups/${wanted}`)) return true;

  return false;
}

function getNextPageToken(payload) {
  return String(
    (payload && (payload.nextPageToken || payload.next_page_token || payload.nextCursor || payload.cursor)) ||
    ""
  );
}

async function scanGroupMembershipsForUser(userId, maxPages = 25) {
  const wantedUserId = Number(userId);
  let pageToken = "";
  let pagesRead = 0;

  do {
    const url =
      `https://apis.roblox.com/cloud/v2/groups/${encodeURIComponent(ROBLOX_GROUP_ID)}/memberships` +
      `?maxPageSize=100` +
      (pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : "");

    const data = await robloxOpenCloudJson(url);
    const memberships = listMembershipsFromPayload(data);
    const exact = memberships.find(membership => getMembershipUserId(membership) === wantedUserId);

    if (exact) {
      const resource = getMembershipResource(exact);
      if (!resource) {
        throw new Error("Group membership was found by scan, but no membership resource/path was returned.");
      }
      return { membership: exact, resource, lookupMethod: "groupScan" };
    }

    pageToken = getNextPageToken(data);
    pagesRead += 1;
  } while (pageToken && pagesRead < maxPages);

  return null;
}

async function getGroupMembershipForUser(userId) {
  const wantedUserId = Number(userId);
  if (!Number.isInteger(wantedUserId) || wantedUserId <= 0) {
    throw new Error("Invalid userId for membership lookup.");
  }

  const filtersToTry = [
    `user == "users/${wantedUserId}"`,
    `user == 'users/${wantedUserId}'`,
    `user=='users/${wantedUserId}'`,
    `users in ["users/${wantedUserId}"]`,
    `users in ['users/${wantedUserId}']`,
    `user in ["users/${wantedUserId}"]`,
    `user in ['users/${wantedUserId}']`,
  ];

  const pathsToTry = [];

  for (const filter of filtersToTry) {
    pathsToTry.push({
      method: `groups/${ROBLOX_GROUP_ID}/memberships filter ${filter}`,
      url:
        `https://apis.roblox.com/cloud/v2/groups/${encodeURIComponent(ROBLOX_GROUP_ID)}/memberships` +
        `?maxPageSize=25&filter=${encodeURIComponent(filter)}`
    });

    pathsToTry.push({
      method: `groups/-/memberships filter ${filter}`,
      url:
        `https://apis.roblox.com/cloud/v2/groups/-/memberships` +
        `?maxPageSize=25&filter=${encodeURIComponent(filter)}`
    });
  }

  const errors = [];

  for (const attempt of pathsToTry) {
    try {
      const data = await robloxOpenCloudJson(attempt.url);
      const memberships = listMembershipsFromPayload(data);

      const exact = memberships.find(membership =>
        getMembershipUserId(membership) === wantedUserId && membershipBelongsToGroup(membership, ROBLOX_GROUP_ID)
      );
      const candidate = exact || memberships.find(membership => getMembershipUserId(membership) === wantedUserId);

      if (!candidate) {
        continue;
      }

      const resource = getMembershipResource(candidate);

      if (!resource) {
        errors.push(`${attempt.method}: membership found but no resource/path returned`);
        continue;
      }

      return {
        membership: candidate,
        resource,
        lookupMethod: attempt.method
      };
    } catch (err) {
      errors.push(`${attempt.method}: ${err.status || "?"} ${err.message}`);
    }
  }

  try {
    const scanned = await scanGroupMembershipsForUser(wantedUserId);
    if (scanned) return scanned;
  } catch (err) {
    errors.push(`groupScan: ${err.status || "?"} ${err.message}`);
  }

  const err = new Error(`No group membership found for user ${wantedUserId}. Lookup attempts: ${errors.slice(-8).join(" | ")}`);
  err.status = 404;
  err.data = { lookupErrors: errors.slice(-20) };
  throw err;
}

function listRolesFromPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return [];
  }

  if (Array.isArray(payload.groupRoles)) return payload.groupRoles;
  if (Array.isArray(payload.roles)) return payload.roles;
  if (Array.isArray(payload.data)) return payload.data;
  return [];
}

async function robloxOpenCloudJson(url, options = {}) {
  if (!ROBLOX_OPEN_CLOUD_API_KEY) {
    throw new Error("ROBLOX_OPEN_CLOUD_API_KEY is not set.");
  }

  const response = await fetch(url, {
    ...options,
    headers: {
      "x-api-key": ROBLOX_OPEN_CLOUD_API_KEY,
      "accept": "application/json",
      ...(options.body ? { "content-type": "application/json" } : {}),
      ...(options.headers || {})
    }
  });

  let data = null;
  const text = await response.text();
  if (text) {
    try {
      data = JSON.parse(text);
    } catch (_) {
      data = { raw: text };
    }
  }

  if (!response.ok) {
    const message =
      data && (data.message || data.error || data.raw)
        ? String(data.message || data.error || data.raw)
        : `Roblox Open Cloud HTTP ${response.status}`;
    const error = new Error(message);
    error.status = response.status;
    error.data = data;
    throw error;
  }

  return data || {};
}

async function getGroupRolesByDisplayName(forceRefresh = false) {
  const now = Date.now();

  if (
    !forceRefresh &&
    cachedGroupRolesByDisplayName &&
    now - cachedGroupRolesFetchedAtMs < GROUP_ROLE_CACHE_TTL_MS
  ) {
    return cachedGroupRolesByDisplayName;
  }

  const rolesByDisplayName = {};
  let pageToken = "";

  do {
    const url =
      `https://apis.roblox.com/cloud/v2/groups/${encodeURIComponent(ROBLOX_GROUP_ID)}/roles` +
      `?maxPageSize=100` +
      (pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : "");

    const data = await robloxOpenCloudJson(url);
    const roles = listRolesFromPayload(data);

    for (const role of roles) {
      const displayName = getGroupRoleDisplayName(role);
      const resource = getGroupRoleResource(role);

      if (displayName && resource) {
        rolesByDisplayName[displayName] = {
          displayName,
          resource,
          id: String(role.id || role.roleId || "").trim(),
          rank: Number(role.rank || 0),
          memberCount: Number(role.memberCount || 0)
        };
      }
    }

    pageToken = String(data.nextPageToken || "");
  } while (pageToken);

  cachedGroupRolesByDisplayName = rolesByDisplayName;
  cachedGroupRolesFetchedAtMs = now;
  return rolesByDisplayName;
}

async function updateRobloxGroupRole(userId, groupRoleResource) {
  const { membership, resource: membershipResource, lookupMethod } = await getGroupMembershipForUser(userId);

  if (membershipHasRole(membership, groupRoleResource)) {
    return {
      alreadyAssigned: true,
      membershipResource,
      lookupMethod,
      membership
    };
  }

  const assignUrl = `https://apis.roblox.com/cloud/v2/${membershipResource}:assignRole`;

  try {
    const assignResult = await robloxOpenCloudJson(assignUrl, {
      method: "POST",
      body: JSON.stringify({
        role: groupRoleResource
      })
    });

    return {
      membershipResource,
      lookupMethod,
      method: "assignRole",
      result: assignResult
    };
  } catch (assignErr) {
    // Older examples used PATCH on the membership itself. Keep this fallback so the
    // sync survives Roblox API behavior differences while still using the correct
    // membership id/resource instead of the player's userId.
    const patchUrl = `https://apis.roblox.com/cloud/v2/${membershipResource}`;

    try {
      const patchResult = await robloxOpenCloudJson(patchUrl, {
        method: "PATCH",
        body: JSON.stringify({
          role: groupRoleResource
        })
      });

      return {
        membershipResource,
        lookupMethod,
        method: "patchRole",
        assignRoleError: {
          status: assignErr.status || null,
          message: assignErr.message,
          details: assignErr.data || null
        },
        result: patchResult
      };
    } catch (patchErr) {
      patchErr.message =
        `assignRole failed (${assignErr.status || "?"}: ${assignErr.message}); ` +
        `PATCH failed (${patchErr.status || "?"}: ${patchErr.message})`;
      patchErr.assignRoleError = assignErr.data || assignErr.message;
      throw patchErr;
    }
  }
}

function assertGroupSyncSecret(req) {
  if (!GROUP_SYNC_SECRET) {
    return false;
  }

  const headerSecret = String(req.get("x-gc-group-sync-secret") || "");
  const bodySecret = String(req.body && req.body.secret || "");
  return headerSecret === GROUP_SYNC_SECRET || bodySecret === GROUP_SYNC_SECRET;
}

app.get("/group-role/status", async (req, res) => {
  if (!assertGroupSyncSecret(req)) {
    return res.status(401).json({ ok: false, error: "Unauthorized." });
  }

  try {
    const roles = await getGroupRolesByDisplayName(req.query.refresh === "1");
    res.json({
      ok: true,
      groupId: ROBLOX_GROUP_ID,
      openCloudKeyPresent: ROBLOX_OPEN_CLOUD_API_KEY.length > 0,
      roleMap: GAME_ROLE_TO_GROUP_ROLE_NAME,
      roleCandidates: GAME_ROLE_TO_GROUP_ROLE_CANDIDATES,
      availableGroupRoles: Object.values(roles)
    });
  } catch (e) {
    res.status(500).json({
      ok: false,
      error: e.message,
      status: e.status || 500,
      details: e.data || null
    });
  }
});

app.post("/group-role/debug-user", async (req, res) => {
  if (!assertGroupSyncSecret(req)) {
    return res.status(401).json({ ok: false, error: "Unauthorized." });
  }

  const userId = Number(req.body && req.body.userId);

  if (!Number.isInteger(userId) || userId <= 0) {
    return res.status(400).json({ ok: false, error: "Invalid userId." });
  }

  try {
    const lookup = await getGroupMembershipForUser(userId);
    res.json({
      ok: true,
      userId,
      groupId: ROBLOX_GROUP_ID,
      membershipResource: lookup.resource,
      membership: lookup.membership
    });
  } catch (e) {
    res.status(e.status || 500).json({
      ok: false,
      userId,
      groupId: ROBLOX_GROUP_ID,
      error: e.message,
      details: e.data || null
    });
  }
});

app.post("/group-role/sync", async (req, res) => {
  if (!assertGroupSyncSecret(req)) {
    return res.status(401).json({ ok: false, error: "Unauthorized." });
  }

  const userId = Number(req.body && req.body.userId);
  const username = String(req.body && req.body.username || "");
  const requestedRole = req.body && req.body.role;
  const gameRole = normalizeGameRole(requestedRole);
  const inGroup = req.body && req.body.inGroup === true;
  const groupRank = Number(req.body && req.body.groupRank);

  if (!Number.isInteger(userId) || userId <= 0) {
    return res.status(400).json({ ok: false, error: "Invalid userId." });
  }

  if (!gameRole) {
    return res.status(400).json({
      ok: false,
      error: "Invalid game role.",
      requestedRole,
      allowedRoles: Object.keys(GAME_ROLE_TO_GROUP_ROLE_CANDIDATES)
    });
  }

  // Do not skip just because Roblox's in-game IsInGroup result says false.
  // Group membership can be newly joined/stale in game servers. The backend does
  // the authoritative Open Cloud membership lookup before changing the role.
  const robloxReportedInGroup = inGroup;

  if (Number.isFinite(groupRank) && groupRank >= 255) {
    return res.json({
      ok: true,
      skipped: true,
      reason: "Player is the group owner; Roblox does not allow changing the group owner's role.",
      userId,
      username,
      gameRole,
      groupRank
    });
  }

  try {
    let roles = await getGroupRolesByDisplayName(false);
    const desiredGroupRoleNames = getGroupRoleCandidates(gameRole);
    let desiredGroupRole = desiredGroupRoleNames.map(name => roles[name]).find(Boolean);

    // If a role was just renamed/created, refresh once before failing.
    if (!desiredGroupRole) {
      roles = await getGroupRolesByDisplayName(true);
      desiredGroupRole = desiredGroupRoleNames.map(name => roles[name]).find(Boolean);
    }

    if (!desiredGroupRole) {
      return res.status(400).json({
        ok: false,
        error: `No matching group role was found for in-game role "${gameRole}" in group ${ROBLOX_GROUP_ID}.`,
        gameRole,
        wantedGroupRoleNames: desiredGroupRoleNames,
        availableGroupRoles: Object.keys(roles)
      });
    }

    const robloxResult = await updateRobloxGroupRole(userId, desiredGroupRole.resource);

    console.log(
      `[GROUP ROLE] ${username || userId} -> gameRole="${gameRole}" groupRole="${desiredGroupRole.displayName}" (${desiredGroupRole.resource})`
    );

    res.json({
      ok: true,
      userId,
      username,
      gameRole,
      robloxReportedInGroup,
      groupRole: desiredGroupRole.displayName,
      groupRoleResource: desiredGroupRole.resource,
      membershipResource: robloxResult && robloxResult.membershipResource,
      lookupMethod: robloxResult && robloxResult.lookupMethod,
      syncMethod: robloxResult && (robloxResult.method || (robloxResult.alreadyAssigned && "alreadyAssigned")),
      roblox: robloxResult
    });
  } catch (e) {
    const errorText = String(e.message || "");
    const detailText = JSON.stringify(e.data || {});
    if (e.status === 400 && (errorText.includes("Cannot change the role for the group owner") || detailText.includes("Cannot change the role for the group owner"))) {
      return res.json({
        ok: true,
        skipped: true,
        reason: "Player is the group owner; Roblox does not allow changing the group owner's role.",
        userId,
        username,
        gameRole
      });
    }

    console.error("[GROUP ROLE] Sync failed", {
      userId,
      username,
      gameRole,
      error: e.message,
      status: e.status || 500,
      details: e.data || null
    });

    res.status(e.status || 500).json({
      ok: false,
      error: e.message,
      status: e.status || 500,
      details: e.data || null
    });
  }
});


// ============================
// Stock News / Generic Market Event Classifier
// ============================
const STOCK_NEWS_CACHE_TTL_MS = 30 * 60 * 1000;
const ALL_MARKET_NEWS_CACHE_TTL_MS = 60 * 1000;
const stockNewsCache = new Map();
let allMarketNewsCache = null;

const CRYPTO_NEWS_SYMBOLS = new Set(["BTC", "ETH", "SOL", "DOGE", "LTC"]);
const CRYPTO_NEWS_PROVIDER_TICKERS = {
  BTC: ["X:BTCUSD", "BTCUSD", "BTC"],
  ETH: ["X:ETHUSD", "ETHUSD", "ETH"],
  SOL: ["X:SOLUSD", "SOLUSD", "SOL"],
  DOGE: ["X:DOGEUSD", "DOGEUSD", "DOGE"],
  LTC: ["X:LTCUSD", "LTCUSD", "LTC"]
};

const newsProviderTickerToAsset = new Map();
for (const [displayTicker, realTicker] of Object.entries(DISPLAY_TICKER_TO_REAL_TICKER)) {
  newsProviderTickerToAsset.set(normalizeStockTicker(realTicker), { ticker: displayTicker, assetType: "stock", providerTicker: realTicker });
  newsProviderTickerToAsset.set(normalizeStockTicker(displayTicker), { ticker: displayTicker, assetType: "stock", providerTicker: realTicker });
}
for (const displayTicker of TICKERS) {
  if (!newsProviderTickerToAsset.has(normalizeStockTicker(displayTicker))) {
    newsProviderTickerToAsset.set(normalizeStockTicker(displayTicker), { ticker: displayTicker, assetType: "stock", providerTicker: getRealTicker(displayTicker) });
  }
}
for (const [symbol, providerTickers] of Object.entries(CRYPTO_NEWS_PROVIDER_TICKERS)) {
  for (const providerTicker of providerTickers) {
    newsProviderTickerToAsset.set(normalizeStockTicker(providerTicker), { ticker: symbol, assetType: "crypto", providerTicker });
  }
}

const STOCK_NEWS_EVENT_TEMPLATES = {
  "very_good_news": [
    {
      title: "Very good news",
      body: "A major positive development is being reported. Traders may expect stronger buying pressure if the market reacts well."
    },
    {
      title: "Very good news",
      body: "Strong bullish news is circulating around this company. The expected reaction is meaningfully positive."
    },
    {
      title: "Very good news",
      body: "A high-impact positive catalyst has appeared. This is the kind of news that can attract aggressive demand."
    }
  ],
  "good_news": [
    {
      title: "Good news",
      body: "A positive company update is being reported. Traders may expect mild to moderate bullish pressure."
    },
    {
      title: "Good news",
      body: "The latest news looks favorable for this company. The expected reaction is positive, but not guaranteed."
    },
    {
      title: "Good news",
      body: "A constructive headline is moving through the market. This could support buying interest."
    }
  ],
  "possibly_good_news": [
    {
      title: "Possibly good news",
      body: "The news appears somewhat favorable, but the market reaction may depend on follow-through and trader interpretation."
    },
    {
      title: "Possibly good news",
      body: "This update leans positive, but it is not a clear breakout catalyst yet."
    },
    {
      title: "Possibly good news",
      body: "A mildly bullish signal is being reported. Traders may want confirmation from price action."
    }
  ],
  "possibly_bad_news": [
    {
      title: "Possibly bad news",
      body: "The news leans negative, but the impact is uncertain. Traders may wait to see whether sellers respond."
    },
    {
      title: "Possibly bad news",
      body: "A mildly bearish signal is being reported. It may pressure the stock if the market takes it seriously."
    },
    {
      title: "Possibly bad news",
      body: "This update could be unfavorable, but the expected reaction is not clearly severe."
    }
  ],
  "bad_news": [
    {
      title: "Bad news",
      body: "A negative company update is being reported. Traders may expect mild to moderate selling pressure."
    },
    {
      title: "Bad news",
      body: "The latest news looks unfavorable for this company. The expected reaction is negative, but not guaranteed."
    },
    {
      title: "Bad news",
      body: "A bearish headline is moving through the market. This could weaken buying interest."
    }
  ],
  "very_bad_news": [
    {
      title: "Very bad news",
      body: "A major negative development is being reported. Traders may expect stronger selling pressure if the market reacts badly."
    },
    {
      title: "Very bad news",
      body: "Strong bearish news is circulating around this company. The expected reaction is meaningfully negative."
    },
    {
      title: "Very bad news",
      body: "A high-impact negative catalyst has appeared. This is the kind of news that can trigger aggressive selling."
    }
  ]
};

function hashStringToIndex(value, length) {
  const text = String(value || "");
  let hash = 0;
  for (let i = 0; i < text.length; i += 1) {
    hash = ((hash << 5) - hash + text.charCodeAt(i)) | 0;
  }
  return Math.abs(hash) % Math.max(1, length || 1);
}

function keywordScore(text) {
  const lower = String(text || "").toLowerCase();
  let score = 0;

  const veryPositive = [
    "beats estimates", "record revenue", "record profit", "raises guidance", "raised guidance", "upgrade", "upgraded",
    "approval", "approved", "breakthrough", "surges", "soars", "jumps", "strong demand", "profit jumps",
    "earnings beat", "wins contract", "major contract", "buyout", "acquisition offer", "strategic partnership"
  ];
  const positive = [
    "beat", "beats", "growth", "grows", "higher", "strong", "bullish", "positive", "profit", "revenue growth",
    "expands", "launches", "partnership", "contract", "dividend increase", "share buyback", "buyback", "guidance"
  ];
  const veryNegative = [
    "misses estimates", "cuts guidance", "cut guidance", "downgrade", "downgraded", "sec investigation",
    "investigation", "lawsuit", "bankruptcy", "delisting", "plunges", "collapses", "crashes", "halts", "recall",
    "fraud", "restatement", "going concern", "offering prices", "dilution"
  ];
  const negative = [
    "miss", "misses", "loss", "losses", "weak", "bearish", "negative", "lower", "decline", "declines",
    "falls", "drops", "slumps", "layoffs", "debt", "offering", "secondary offering", "warns", "risk", "cuts"
  ];

  for (const phrase of veryPositive) if (lower.includes(phrase)) score += 1.6;
  for (const phrase of positive) if (lower.includes(phrase)) score += 0.7;
  for (const phrase of veryNegative) if (lower.includes(phrase)) score -= 1.6;
  for (const phrase of negative) if (lower.includes(phrase)) score -= 0.7;

  return score;
}

function classifyNewsEvent(article, realTicker) {
  const insights = Array.isArray(article.insights) ? article.insights : [];
  const tickerUpper = normalizeStockTicker(realTicker);
  const matchingInsight = insights.find(insight => normalizeStockTicker(insight && insight.ticker) === tickerUpper) || insights[0] || null;
  const sentiment = matchingInsight && matchingInsight.sentiment ? String(matchingInsight.sentiment).toLowerCase() : "";
  const reasoning = matchingInsight && matchingInsight.sentiment_reasoning ? String(matchingInsight.sentiment_reasoning) : "";

  let score = 0;
  if (sentiment === "positive") score += 1.25;
  if (sentiment === "negative") score -= 1.25;

  score += keywordScore(`${article.title || ""} ${article.description || ""} ${reasoning}`);

  let eventType = "possibly_good_news";
  if (score >= 3.0) eventType = "very_good_news";
  else if (score >= 1.25) eventType = "good_news";
  else if (score >= 0.25) eventType = "possibly_good_news";
  else if (score <= -3.0) eventType = "very_bad_news";
  else if (score <= -1.25) eventType = "bad_news";
  else if (score <= -0.25) eventType = "possibly_bad_news";
  else if (sentiment === "negative") eventType = "possibly_bad_news";

  const templates = STOCK_NEWS_EVENT_TEMPLATES[eventType] || STOCK_NEWS_EVENT_TEMPLATES.possibly_good_news;
  const template = templates[hashStringToIndex(article.id || article.title || article.published_utc, templates.length)];

  return {
    eventType,
    eventTitle: template.title,
    eventText: template.body,
    expectedResult: eventType.includes("good") ? "bullish" : "bearish",
    score: Number(score.toFixed(2)),
    providerSentiment: sentiment || null,
    providerReasoning: null
  };
}

function normalizeMassiveNewsArticle(article, displayTicker, providerTicker, assetType = "stock") {
  const classification = classifyNewsEvent(article, providerTicker);
  return {
    id: String(article.id || `market-news-${article.published_utc || Date.now()}`),
    displayTicker,
    assetType: assetType === "crypto" ? "crypto" : "stock",
    realTicker: "",
    eventType: classification.eventType,
    eventTitle: classification.eventTitle,
    eventText: classification.eventText,
    expectedResult: classification.expectedResult,
    score: classification.score,
    providerSentiment: classification.providerSentiment,
    providerReasoning: classification.providerReasoning,
    source: "Market News Desk",
    publishedAt: article.published_utc || null,
    headline: "",
    summary: "",
    articleUrl: ""
  };
}

function resolveNewsAssets(article) {
  const rawTickers = Array.isArray(article && article.tickers) ? article.tickers : [];
  const assets = [];
  const seen = new Set();

  for (const rawTicker of rawTickers) {
    const asset = newsProviderTickerToAsset.get(normalizeStockTicker(rawTicker));
    if (!asset) continue;

    const key = `${asset.assetType}:${asset.ticker}`;
    if (seen.has(key)) continue;
    seen.add(key);

    assets.push({
      ticker: asset.ticker,
      assetType: asset.assetType,
      providerTicker: rawTicker || asset.providerTicker
    });
  }

  return assets;
}

async function fetchAllMarketNews() {
  if (allMarketNewsCache && Date.now() - allMarketNewsCache.fetchedAt < ALL_MARKET_NEWS_CACHE_TTL_MS) {
    return { ...allMarketNewsCache.data, cached: true };
  }

  if (!MASSIVE_API_KEY) {
    return {
      success: false,
      provider: "Market Signals",
      articles: [],
      error: "Market news service is not configured."
    };
  }

  const url = `${MASSIVE_BASE_URL}/v2/reference/news?order=desc&sort=published_utc&limit=100&apiKey=${encodeURIComponent(MASSIVE_API_KEY)}`;
  const response = await fetchJsonWithTimeout(url).catch(e => ({ ok: false, status: 0, data: { error: e.message } }));

  if (!response.ok || !response.data || !Array.isArray(response.data.results)) {
    return {
      success: false,
      provider: "Market Signals",
      articles: [],
      status: response.status,
      error: "Market news service temporarily unavailable."
    };
  }

  const normalized = [];
  const seen = new Set();

  for (const article of response.data.results) {
    if (!article || typeof article !== "object") continue;

    for (const asset of resolveNewsAssets(article)) {
      const articleId = String(article.id || article.published_utc || "market-news");
      const key = `${articleId}:${asset.assetType}:${asset.ticker}`;
      if (seen.has(key)) continue;
      seen.add(key);

      normalized.push(normalizeMassiveNewsArticle(
        article,
        asset.ticker,
        asset.providerTicker,
        asset.assetType
      ));
    }
  }

  normalized.sort((a, b) => {
    const aTime = Date.parse(a.publishedAt || "") || 0;
    const bTime = Date.parse(b.publishedAt || "") || 0;
    return bTime - aTime;
  });

  const data = {
    success: true,
    provider: "Market Signals",
    fetchedAt: Date.now(),
    articles: normalized.slice(0, 60)
  };

  allMarketNewsCache = { fetchedAt: Date.now(), data };
  return data;
}

async function fetchCryptoNews(symbol) {
  const ticker = normalizeStockTicker(symbol);
  if (!CRYPTO_NEWS_SYMBOLS.has(ticker)) {
    return { success: false, ticker, assetType: "crypto", articles: [], error: "Unknown crypto symbol." };
  }
  if (!MASSIVE_API_KEY) {
    return {
      success: false,
      ticker,
      assetType: "crypto",
      provider: "Market Signals",
      articles: [],
      error: "Market news service is not configured."
    };
  }

  const cacheKey = `crypto:${ticker}`;
  const cached = stockNewsCache.get(cacheKey);
  if (cached && Date.now() - cached.fetchedAt < STOCK_NEWS_CACHE_TTL_MS) {
    return { ...cached.data, cached: true };
  }

  const candidates = CRYPTO_NEWS_PROVIDER_TICKERS[ticker] || [];
  let providerSucceeded = false;
  let providerStatus = 0;
  let articles = [];

  for (const providerTicker of candidates) {
    const url = `${MASSIVE_BASE_URL}/v2/reference/news?ticker=${encodeURIComponent(providerTicker)}&order=desc&sort=published_utc&limit=10&apiKey=${encodeURIComponent(MASSIVE_API_KEY)}`;
    const response = await fetchJsonWithTimeout(url).catch(e => ({ ok: false, status: 0, data: { error: e.message } }));
    providerStatus = response.status || providerStatus;

    if (!response.ok || !response.data || !Array.isArray(response.data.results)) {
      continue;
    }

    providerSucceeded = true;
    const candidateKeys = new Set(candidates.map(normalizeStockTicker));
    const matching = response.data.results
      .filter(article => article && typeof article === "object")
      .filter(article => {
        const articleTickers = Array.isArray(article.tickers) ? article.tickers.map(normalizeStockTicker) : [];
        return articleTickers.length === 0 || articleTickers.some(value => candidateKeys.has(value));
      });

    if (matching.length > 0) {
      articles = matching.slice(0, 8).map(article =>
        normalizeMassiveNewsArticle(article, ticker, providerTicker, "crypto")
      );
      break;
    }
  }

  if (articles.length === 0) {
    const allNews = await fetchAllMarketNews();
    if (allNews && allNews.success === true && Array.isArray(allNews.articles)) {
      articles = allNews.articles
        .filter(article => article && article.assetType === "crypto" && article.displayTicker === ticker)
        .slice(0, 8);
      providerSucceeded = true;
    }
  }

  if (!providerSucceeded) {
    return {
      success: false,
      ticker,
      assetType: "crypto",
      provider: "Market Signals",
      articles: [],
      status: providerStatus,
      error: "Market news service temporarily unavailable."
    };
  }

  const data = {
    success: true,
    ticker,
    assetType: "crypto",
    provider: "Market Signals",
    fetchedAt: Date.now(),
    articles
  };

  stockNewsCache.set(cacheKey, { fetchedAt: Date.now(), data });
  return data;
}

async function fetchStockNews(displayTicker) {
  const ticker = normalizeStockTicker(displayTicker);
  if (!ticker) {
    return { success: false, error: "Missing ticker." };
  }
  if (!isRealStockTicker(ticker)) {
    return { success: false, ticker, error: "Unknown real-data stock ticker." };
  }
  if (!MASSIVE_API_KEY) {
    return {
      success: false,
      ticker,
      realTicker: "",
      provider: "Market Signals",
      error: "Market news service is not configured."
    };
  }

  const realTicker = getRealTicker(ticker);
  const cacheKey = `${ticker}:${realTicker}`;
  const cached = stockNewsCache.get(cacheKey);
  if (cached && Date.now() - cached.fetchedAt < STOCK_NEWS_CACHE_TTL_MS) {
    return { ...cached.data, cached: true };
  }

  const url = `${MASSIVE_BASE_URL}/v2/reference/news?ticker=${encodeURIComponent(realTicker)}&order=desc&sort=published_utc&limit=10&apiKey=${encodeURIComponent(MASSIVE_API_KEY)}`;
  const response = await fetchJsonWithTimeout(url).catch(e => ({ ok: false, status: 0, data: { error: e.message } }));

  if (!response.ok || !response.data || !Array.isArray(response.data.results)) {
    const message = response.data && (response.data.error || response.data.message) ? (response.data.error || response.data.message) : "News provider returned no usable results.";
    return {
      success: false,
      ticker,
      realTicker: "",
      provider: "Market Signals",
      status: response.status,
      error: "Market news service temporarily unavailable."
    };
  }

  const articles = response.data.results
    .filter(article => article && typeof article === "object")
    .filter(article => {
      const tickers = Array.isArray(article.tickers) ? article.tickers.map(normalizeStockTicker) : [];
      return tickers.length === 0 || tickers.includes(normalizeStockTicker(realTicker));
    })
    .slice(0, 8)
    .map(article => normalizeMassiveNewsArticle(article, ticker, realTicker, "stock"));

  const data = {
    success: true,
    ticker,
    assetType: "stock",
    realTicker: "",
    provider: "Market Signals",
    fetchedAt: Date.now(),
    articles
  };

  stockNewsCache.set(cacheKey, { fetchedAt: Date.now(), data });
  return data;
}

// ============================
// Routes
// ============================
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    wsReady,
    wsActive: Date.now() - lastWsTradeTime < WS_QUIET_THRESHOLD_MS,
    lastWsTradeMsAgo: Date.now() - lastWsTradeTime,
    cached: Object.keys(priceCache).length,
    twelveDataCreditsUsedToday,
    twelveDataCandleCreditsUsedToday,
    maxTwelveDataCandleCreditsPerDay: MAX_TWELVE_DATA_CANDLE_CREDITS_PER_DAY,
    twelveDataPollingEnabled: ENABLE_TWELVE_DATA_POLLING,
    twelveDataCandlesEnabled: ENABLE_TWELVE_DATA_CANDLES,
    twelveDataMarketCandleBackupEnabled: ENABLE_TWELVE_DATA_MARKET_CANDLE_BACKUP,
    twelveDataQueueDepth: tdQueueDepth(),
    twelveDataCallsInLastMinute: tdCallsInLastMinute(),
    candleCacheEntries: Object.keys(candleCache).length,
    cryptoCached: Object.keys(cryptoPriceCache).length,
    cryptoSourceSample: cryptoPriceCache.BTC && cryptoPriceCache.BTC.source,
    cryptoCandleCacheEntries: Object.keys(cryptoCandleCache).length,
    massiveNewsApiKeyPresent: Boolean(MASSIVE_API_KEY),
    stockNewsCacheEntries: stockNewsCache.size,
    yahooOnDemandQuotes: ENABLE_YAHOO_ON_DEMAND_QUOTES,
    yahooBackgroundPolling: ENABLE_YAHOO_QUOTE_POLLING,
    allYahooQuotesMsAgo: allYahooQuotesFetchedAtMs ? Date.now() - allYahooQuotesFetchedAtMs : null,
    yahooQuoteInFlight: yahooQuoteInFlight.size,
    stockCandleInFlight: stockCandleInFlight.size,
    isRegularMarketHours: isRegularMarketHours(),
    isExtendedHours: isExtendedHours(),
    marketStatus: getMarketSessionStatus()
  });
});

app.get("/market/status", (req, res) => {
  res.json(getMarketSessionStatus());
});

app.get("/crypto/prices", async (req, res) => {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");

  const rawSymbols = String(req.query.symbols || "BTC,ETH,SOL,DOGE,LTC")
    .toUpperCase()
    .replace(/\s+/g, "")
    .replace(/\+/g, ",");

  const symbols = rawSymbols
    .split(",")
    .filter(Boolean)
    .filter(symbol => CRYPTO_SYMBOLS.includes(symbol));

  const result = await fetchCryptoPrices(
    symbols.length > 0 ? symbols : CRYPTO_SYMBOLS
  );

  res.json(result);
});

app.get("/crypto/candles", async (req, res) => {
  const symbol = normalizeSymbol(
    req.query.symbol || req.query.ticker || "BTC"
  );

  const interval = String(req.query.interval || "1m").toLowerCase();

  const result = await fetchCryptoCandles(symbol, interval);

  res.json(result);
});

app.get("/crypto/debug", async (req, res) => {
  res.set("Cache-Control", "no-store");

  const symbol = normalizeSymbol(req.query.symbol || "BTC");
  if (!CRYPTO_SYMBOLS.includes(symbol)) {
    return res.json({ error: "Unsupported debug symbol." });
  }

  const result = await fetchCryptoPrices([symbol]);
  const info = result.prices && result.prices[symbol];

  res.json({
    symbol,
    price: info && info.price,
    change24h: info && info.change24h,
    source: info && info.source,
    providerTimestamp: info && info.lastUpdated,
    receivedAtMs: info && info.receivedAtMs,
    ageMs: info && info.receivedAtMs ? Math.max(0, Date.now() - info.receivedAtMs) : null,
    streamHealthy: isCryptoStreamHealthy(),
    websocketReadyState: cryptoTickerWs ? cryptoTickerWs.readyState : null,
    websocketSource: CRYPTO_STREAM_SOURCES[cryptoTickerWsSourceIndex].name,
    websocketLastMessageAgeMs: cryptoTickerWsLastMessageAt > 0 ? Math.max(0, Date.now() - cryptoTickerWsLastMessageAt) : null,
    cacheSymbols: Object.keys(cryptoPriceCache),
    stale: result.stale === true,
    providerError: result.providerError || null
  });
});

app.get("/float", async (req, res) => {
  const ticker = String(req.query.ticker || "").toUpperCase();

  if (!ticker) {
    return res.json({ error: "Missing ticker." });
  }

  if (!isRealStockTicker(ticker)) {
    return res.json({
      ticker,
      error: "Unknown stock ticker."
    });
  }

  try {
    await fetchYahooQuotesDeduped([ticker]);
    await enrichYahooFloatMetadataForTickers([ticker]);
  } catch (e) {
    console.warn(`[FLOAT] refresh failed for ${ticker}: ${e.message}`);
  }

  const row = priceCache[ticker] || {};
  return res.json({
    ticker,
    realTicker: getRealTicker(ticker),
    fmpEnabled: Boolean(FMP_API_KEY),
    price: row.price,
    marketCap: row.marketCap,
    sharesOutstanding: row.sharesOutstanding,
    floatShares: row.floatShares,
    publicFloat: row.publicFloat,
    source: row.source,
    marketState: row.marketState,
    lastUpdated: row.lastUpdated,
    fetchedAt: row.fetchedAt
  });
});


app.get("/news", async (req, res) => {
  const ticker = String(req.query.ticker || "").toUpperCase();
  const assetType = String(req.query.assetType || "stock").toLowerCase();
  const result = assetType === "crypto"
    ? await fetchCryptoNews(ticker)
    : await fetchStockNews(ticker);
  res.json(result);
});

app.get("/news/all", async (req, res) => {
  const result = await fetchAllMarketNews();
  res.json(result);
});


// ============================
// Commodity market data
// ============================
// Twelve Data is the primary provider because it has an official commodities API
// with real-time quotes and intraday/history on the free tier. Yahoo commodity
// futures are kept as a no-key fallback so the game remains usable if the free
// Twelve Data allowance is exhausted or a specific commodity is not enabled.
const COMMODITY_DEFINITIONS = {
  GOLD: {
    name: "Gold",
    unit: "troy ounce",
    twelveCandidates: ["XAU/USD"],
    yahooSymbol: "GC=F",
    minValidPrice: 100,
    maxValidPrice: 20000
  },
  SILVER: {
    name: "Silver",
    unit: "troy ounce",
    twelveCandidates: ["XAG/USD"],
    yahooSymbol: "SI=F",
    minValidPrice: 1,
    maxValidPrice: 1000
  },
  OIL: {
    name: "WTI Crude Oil",
    unit: "barrel",
    // IMPORTANT: never use bare "WTI" here. Twelve Data can resolve that as
    // W&T Offshore stock instead of the WTI crude-oil commodity pair.
    twelveCandidates: ["WTI/USD"],
    yahooSymbol: "CL=F",
    minValidPrice: 10,
    maxValidPrice: 500
  }
};

const COMMODITY_TICKERS = Object.keys(COMMODITY_DEFINITIONS);
const commodityPriceCache = {};
const commodityResolvedTwelveSymbol = {};
let commodityRefreshInProgress = null;
let commodityLastTwelveRestRefreshAt = 0;
let commodityWs = null;
let commodityWsLastMessageAt = 0;
let commodityWsReconnectTimer = null;
let commodityWsHeartbeatTimer = null;
const commodityCandleRequestInFlight = new Map();
const commodityTickerWarmupInFlight = new Set();
const COMMODITY_CANDLE_WARMUP_INTERVALS = ["1min", "5min", "15min", "30min", "1h", "1day"];

const COMMODITY_QUOTE_FRESH_MS = 5 * 1000;
const COMMODITY_TWELVE_REST_INTERVAL_MS = 60 * 1000;
const COMMODITY_WS_STALE_MS = 20 * 1000;

function normalizeCommodityTicker(value) {
  return String(value || "").trim().toUpperCase();
}

function isCommodityTicker(value) {
  return Boolean(COMMODITY_DEFINITIONS[normalizeCommodityTicker(value)]);
}

function commodityDisplayTickerForProviderSymbol(symbol) {
  const normalized = String(symbol || "").trim().toUpperCase();
  for (const [displayTicker, definition] of Object.entries(COMMODITY_DEFINITIONS)) {
    if (definition.twelveCandidates.some(candidate => candidate.toUpperCase() === normalized)) {
      return displayTicker;
    }
  }
  return null;
}

function commodityPriceIsValid(displayTicker, value) {
  const definition = COMMODITY_DEFINITIONS[normalizeCommodityTicker(displayTicker)];
  const price = toNumber(value);
  if (!definition || price === null || price <= 0) return false;
  const min = Number(definition.minValidPrice) || 0;
  const max = Number(definition.maxValidPrice) || Number.POSITIVE_INFINITY;
  return price >= min && price <= max;
}

function commodityRowIsFresh(row, maxAgeMs = COMMODITY_QUOTE_FRESH_MS) {
  return Boolean(
    row &&
    commodityPriceIsValid(row.ticker, row.price) &&
    Date.now() - Number(row.receivedAtMs || 0) <= maxAgeMs
  );
}

function updateCommodityPrice(displayTicker, payload) {
  displayTicker = normalizeCommodityTicker(displayTicker);
  const definition = COMMODITY_DEFINITIONS[displayTicker];
  if (!definition) return false;

  const price = toNumber(payload && payload.price);
  if (!commodityPriceIsValid(displayTicker, price)) {
    console.warn(`[COMMODITY] Rejected invalid ${displayTicker} price: ${price}`);
    return false;
  }

  const existing = commodityPriceCache[displayTicker] || {};
  const prevClose = toNumber(payload.prevClose) ?? toNumber(existing.prevClose) ?? price;
  const suppliedChange = toNumber(payload.changePct);
  const changePct = suppliedChange !== null
    ? suppliedChange
    : (prevClose > 0 ? ((price - prevClose) / prevClose) * 100 : 0);
  const lastUpdated = Number(payload.lastUpdated) > 0
    ? Number(payload.lastUpdated)
    : Math.floor(Date.now() / 1000);

  commodityPriceCache[displayTicker] = {
    ticker: displayTicker,
    name: definition.name,
    unit: definition.unit,
    assetType: "commodity",
    price,
    prevClose,
    changePct: Number(changePct.toFixed(4)),
    lastUpdated,
    fetchedAt: Date.now(),
    receivedAtMs: Date.now(),
    source: String(payload.source || existing.source || "Commodity market data"),
    providerSymbol: payload.providerSymbol || existing.providerSymbol || null
  };

  return true;
}

async function fetchTwelveDataCommodityQuote(displayTicker) {
  if (!TWELVE_DATA_API_KEY) throw new Error("TWELVE_DATA_API_KEY is not configured.");

  const definition = COMMODITY_DEFINITIONS[displayTicker];
  if (!definition) throw new Error("Unknown commodity ticker.");

  const resolvedCandidate = commodityResolvedTwelveSymbol[displayTicker];
  const resolved = definition.twelveCandidates.includes(resolvedCandidate) ? resolvedCandidate : null;
  const candidates = resolved
    ? [resolved, ...definition.twelveCandidates.filter(symbol => symbol !== resolved)]
    : definition.twelveCandidates;

  let lastError = null;
  for (const providerSymbol of candidates) {
    try {
      const url =
        `https://api.twelvedata.com/quote?symbol=${encodeURIComponent(providerSymbol)}` +
        `&timezone=UTC&apikey=${encodeURIComponent(TWELVE_DATA_API_KEY)}`;
      const data = await tdRequest(url, TD_PRIORITY.quote);
      if (!data || data.status === "error") {
        throw new Error(data && data.message ? data.message : "No Twelve Data commodity quote.");
      }

      const returnedSymbol = String(data.symbol || providerSymbol).trim().toUpperCase();
      if (returnedSymbol !== providerSymbol.toUpperCase()) {
        throw new Error(`Twelve Data returned ${returnedSymbol} for requested ${providerSymbol}.`);
      }

      const price = toNumber(data.close) ?? toNumber(data.price);
      if (!commodityPriceIsValid(displayTicker, price)) {
        throw new Error(`Twelve Data returned an invalid ${displayTicker} price: ${price}`);
      }

      const prevCloseCandidate = toNumber(data.previous_close);
      const prevClose = commodityPriceIsValid(displayTicker, prevCloseCandidate) ? prevCloseCandidate : price;
      const percentChange = toNumber(data.percent_change);
      commodityResolvedTwelveSymbol[displayTicker] = providerSymbol;
      updateCommodityPrice(displayTicker, {
        price,
        prevClose,
        changePct: percentChange,
        lastUpdated: Number(data.timestamp) || Math.floor(Date.now() / 1000),
        source: "Twelve Data commodities",
        providerSymbol
      });
      return commodityPriceCache[displayTicker];
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error("No Twelve Data commodity symbol returned data.");
}

async function fetchYahooCommodityQuote(displayTicker) {
  const definition = COMMODITY_DEFINITIONS[displayTicker];
  if (!definition) throw new Error("Unknown commodity ticker.");

  const url =
    `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(definition.yahooSymbol)}` +
    `?range=5d&interval=1m&includePrePost=true&_=${Date.now()}`;
  const response = await fetchJsonWithTimeout(
    url,
    { headers: { Accept: "application/json", "User-Agent": "Mozilla/5.0" } },
    12000
  );

  if (!response.ok) throw new Error(`Yahoo commodity HTTP ${response.status}`);
  const chart = response.data && response.data.chart;
  const result = chart && Array.isArray(chart.result) && chart.result[0];
  if (!result) throw new Error(chart?.error?.description || "No Yahoo commodity quote result.");

  const meta = result.meta || {};
  const timestamps = Array.isArray(result.timestamp) ? result.timestamp : [];
  const quote = result.indicators && result.indicators.quote && result.indicators.quote[0];
  const closes = quote && Array.isArray(quote.close) ? quote.close : [];

  let latestPrice = toNumber(meta.regularMarketPrice);
  let latestTimestamp = Number(meta.regularMarketTime) || 0;
  for (let index = closes.length - 1; index >= 0; index--) {
    const candidate = toNumber(closes[index]);
    if (candidate !== null && candidate > 0) {
      latestPrice = candidate;
      latestTimestamp = Number(timestamps[index]) || latestTimestamp;
      break;
    }
  }

  if (!commodityPriceIsValid(displayTicker, latestPrice)) {
    throw new Error(`Yahoo returned an invalid ${displayTicker} price: ${latestPrice}`);
  }
  const rawPrevClose = toNumber(meta.chartPreviousClose) ?? toNumber(meta.previousClose);
  const prevClose = commodityPriceIsValid(displayTicker, rawPrevClose) ? rawPrevClose : latestPrice;

  updateCommodityPrice(displayTicker, {
    price: latestPrice,
    prevClose,
    lastUpdated: latestTimestamp || Math.floor(Date.now() / 1000),
    source: "Yahoo Finance commodity futures fallback",
    providerSymbol: definition.yahooSymbol
  });
  return commodityPriceCache[displayTicker];
}

async function refreshCommodityQuotes(force = false) {
  if (commodityRefreshInProgress) return commodityRefreshInProgress;

  const tickersToRefresh = force
    ? COMMODITY_TICKERS.slice()
    : COMMODITY_TICKERS.filter(ticker => !commodityRowIsFresh(commodityPriceCache[ticker]));

  if (tickersToRefresh.length === 0) return commodityPriceCache;

  // Quotes and candles must describe the same instruments. The old path mixed
  // Twelve Data spot quotes (XAU/USD, XAG/USD, WTI/USD) with Yahoo futures
  // candles (GC=F, SI=F, CL=F), which guaranteed visible chart/quote drift.
  // Yahoo futures are now authoritative for both the displayed quote and chart.
  commodityRefreshInProgress = (async () => {
    const results = await Promise.allSettled(tickersToRefresh.map(fetchYahooCommodityQuote));
    results.forEach((result, index) => {
      if (result.status === "rejected") {
        const ticker = tickersToRefresh[index];
        console.warn(`[COMMODITY] Yahoo ${ticker} quote failed: ${result.reason?.message || result.reason}`);
      }
    });
    return commodityPriceCache;
  })().finally(() => {
    commodityRefreshInProgress = null;
  });

  return commodityRefreshInProgress;
}

function connectCommodityTickerStream() {
  if (!TWELVE_DATA_API_KEY) {
    console.log("[COMMODITY] Twelve Data WebSocket disabled: TWELVE_DATA_API_KEY is not set.");
    return;
  }

  if (commodityWsReconnectTimer) {
    clearTimeout(commodityWsReconnectTimer);
    commodityWsReconnectTimer = null;
  }
  if (commodityWsHeartbeatTimer) {
    clearInterval(commodityWsHeartbeatTimer);
    commodityWsHeartbeatTimer = null;
  }
  if (commodityWs) {
    try { commodityWs.removeAllListeners(); commodityWs.terminate(); } catch (_) {}
    commodityWs = null;
  }

  const url = `wss://ws.twelvedata.com/v1/quotes/price?apikey=${encodeURIComponent(TWELVE_DATA_API_KEY)}`;
  const socket = new WebSocket(url);
  commodityWs = socket;

  socket.on("open", () => {
    const symbols = [...new Set(Object.values(COMMODITY_DEFINITIONS).flatMap(definition => definition.twelveCandidates))];
    socket.send(JSON.stringify({ action: "subscribe", params: { symbols: symbols.join(",") } }));
    commodityWsHeartbeatTimer = setInterval(() => {
      if (socket.readyState === WebSocket.OPEN) {
        try { socket.send(JSON.stringify({ action: "heartbeat" })); } catch (_) {}
      }
    }, 10000);
    console.log(`[COMMODITY] Twelve Data stream subscribed to ${symbols.join(", ")}`);
  });

  socket.on("message", raw => {
    let decoded;
    try { decoded = JSON.parse(String(raw)); } catch (_) { return; }
    const events = Array.isArray(decoded) ? decoded : [decoded];

    for (const event of events) {
      if (!event || toNumber(event.price) === null) continue;
      const displayTicker = commodityDisplayTickerForProviderSymbol(event.symbol);
      if (!displayTicker) continue;

      const currentResolved = commodityResolvedTwelveSymbol[displayTicker];
      if (currentResolved && String(currentResolved).toUpperCase() !== String(event.symbol).toUpperCase()) continue;
      if (!currentResolved) {
        // OIL has several provider aliases. Accept whichever valid candidate begins
        // streaming first, then lock this display ticker to that provider symbol.
        commodityResolvedTwelveSymbol[displayTicker] = event.symbol;
      }

      commodityWsLastMessageAt = Date.now();
      updateCommodityPrice(displayTicker, {
        price: event.price,
        lastUpdated: Number(event.timestamp) || Math.floor(Date.now() / 1000),
        source: "Twelve Data commodities WebSocket",
        providerSymbol: event.symbol
      });
    }
  });

  const scheduleReconnect = () => {
    if (commodityWs === socket) commodityWs = null;
    if (commodityWsHeartbeatTimer) {
      clearInterval(commodityWsHeartbeatTimer);
      commodityWsHeartbeatTimer = null;
    }
    if (!commodityWsReconnectTimer) {
      commodityWsReconnectTimer = setTimeout(() => {
        commodityWsReconnectTimer = null;
        connectCommodityTickerStream();
      }, 7000);
    }
  };

  socket.on("close", scheduleReconnect);
  socket.on("error", error => {
    console.warn(`[COMMODITY] Twelve Data stream error: ${error.message}`);
  });
}

function parseUtcCommodityTimestamp(datetime) {
  if (!datetime) return null;
  const normalized = String(datetime).trim().replace(" ", "T");
  const timestamp = Date.parse(normalized.endsWith("Z") ? normalized : `${normalized}Z`);
  return Number.isFinite(timestamp) ? Math.floor(timestamp / 1000) : null;
}

async function fetchTwelveDataCommodityCandles(displayTicker, interval, limit = 200) {
  if (!TWELVE_DATA_API_KEY) throw new Error("TWELVE_DATA_API_KEY is not configured.");
  const definition = COMMODITY_DEFINITIONS[displayTicker];
  if (!definition) throw new Error("Unknown commodity ticker.");

  const resolvedCandidate = commodityResolvedTwelveSymbol[displayTicker];
  const resolved = definition.twelveCandidates.includes(resolvedCandidate) ? resolvedCandidate : null;
  const candidates = resolved
    ? [resolved, ...definition.twelveCandidates.filter(symbol => symbol !== resolved)]
    : definition.twelveCandidates;
  let lastError = null;

  for (const providerSymbol of candidates) {
    try {
      const url =
        `https://api.twelvedata.com/time_series?symbol=${encodeURIComponent(providerSymbol)}` +
        `&interval=${encodeURIComponent(interval)}&outputsize=${Math.max(20, Math.min(500, limit))}` +
        `&timezone=UTC&apikey=${encodeURIComponent(TWELVE_DATA_API_KEY)}`;
      const data = await tdRequest(url, TD_PRIORITY.candle);
      if (!data || data.status === "error" || !Array.isArray(data.values)) {
        throw new Error(data && data.message ? data.message : "No Twelve Data commodity candles.");
      }

      const candles = data.values
        .slice()
        .reverse()
        .map(value => {
          const o = toNumber(value.open);
          const h = toNumber(value.high);
          const l = toNumber(value.low);
          const c = toNumber(value.close);
          if (
            !commodityPriceIsValid(displayTicker, o) ||
            !commodityPriceIsValid(displayTicker, h) ||
            !commodityPriceIsValid(displayTicker, l) ||
            !commodityPriceIsValid(displayTicker, c)
          ) return null;
          const ts = Number(value.timestamp) || parseUtcCommodityTimestamp(value.datetime);
          return {
            t: ts ? formatSyntheticStockCandleTime(ts * 1000, interval) : String(value.datetime || ""),
            ts: ts || undefined,
            session: "commodity",
            o: roundCandleNumber(o),
            h: roundCandleNumber(h),
            l: roundCandleNumber(l),
            c: roundCandleNumber(c),
            v: toNumber(value.volume) || 0
          };
        })
        .filter(Boolean)
        .slice(-limit);

      if (candles.length === 0) throw new Error("Twelve Data returned no usable commodity candles.");
      commodityResolvedTwelveSymbol[displayTicker] = providerSymbol;
      setCachedCandles(displayTicker, interval, candles, "Twelve Data commodities");
      return candles;
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error("No Twelve Data commodity candle symbol returned data.");
}

async function fetchYahooCommodityCandles(displayTicker, interval, limit = 200) {
  const definition = COMMODITY_DEFINITIONS[displayTicker];
  const cfg = YAHOO_INTERVALS[interval];
  if (!definition) throw new Error("Unknown commodity ticker.");
  if (!cfg) throw new Error("Unsupported commodity candle interval.");

  let lastError = null;
  for (const host of ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]) {
    try {
      const url =
        `https://${host}/v8/finance/chart/${encodeURIComponent(definition.yahooSymbol)}` +
        `?range=${encodeURIComponent(cfg.range)}&interval=${encodeURIComponent(cfg.interval)}` +
        `&includePrePost=true&events=div%2Csplits&_=${Date.now()}`;
      const response = await fetchJsonWithTimeout(
        url,
        { headers: { Accept: "application/json", "User-Agent": "Mozilla/5.0" } },
        6500
      );
      if (!response.ok) throw new Error(`Yahoo commodity candle HTTP ${response.status}`);

      const chart = response.data && response.data.chart;
      const result = chart && Array.isArray(chart.result) && chart.result[0];
      if (!result) throw new Error(chart?.error?.description || "No Yahoo commodity candle result.");
      const timestamps = Array.isArray(result.timestamp) ? result.timestamp : [];
      const quote = result.indicators && result.indicators.quote && result.indicators.quote[0];
      if (!quote || timestamps.length === 0) throw new Error("Yahoo commodity candles were empty.");

      const candles = [];
      for (let index = 0; index < timestamps.length; index++) {
        const o = toNumber(quote.open && quote.open[index]);
        const h = toNumber(quote.high && quote.high[index]);
        const l = toNumber(quote.low && quote.low[index]);
        const c = toNumber(quote.close && quote.close[index]);
        if (
          !commodityPriceIsValid(displayTicker, o) ||
          !commodityPriceIsValid(displayTicker, h) ||
          !commodityPriceIsValid(displayTicker, l) ||
          !commodityPriceIsValid(displayTicker, c)
        ) continue;
        const ts = Number(timestamps[index]);
        candles.push({
          t: formatSyntheticStockCandleTime(ts * 1000, interval),
          ts,
          session: "commodity",
          o: roundCandleNumber(o),
          h: roundCandleNumber(Math.max(h, o, c, l)),
          l: roundCandleNumber(Math.min(l, o, c, h)),
          c: roundCandleNumber(c),
          v: Math.max(0, toNumber(quote.volume && quote.volume[index]) || 0)
        });
      }

      const output = sanitizeCommodityCandleSeries(displayTicker, candles, interval).slice(-limit);
      if (output.length === 0) throw new Error("Yahoo returned no usable commodity candles.");
      setCachedCandles(displayTicker, interval, output, "Yahoo Finance commodity futures");
      return output;
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError || new Error("Yahoo commodity candles failed.");
}

function getAnyCommodityCandleCacheEntry(ticker, interval) {
  return candleCache[`${ticker}:${interval}`] || null;
}

async function getCommodityCandlesFast(ticker, interval) {
  const fresh = getCachedCandleEntry(ticker, interval);
  if (fresh && Array.isArray(fresh.data) && fresh.data.length > 0) {
    return { entry: fresh, candles: fresh.data, cached: true };
  }

  const key = `${ticker}:${interval}`;
  if (!commodityCandleRequestInFlight.has(key)) {
    const request = fetchYahooCommodityCandles(ticker, interval, 200)
      .then(candles => ({
        entry: getAnyCommodityCandleCacheEntry(ticker, interval),
        candles,
        cached: false
      }))
      .finally(() => commodityCandleRequestInFlight.delete(key));
    commodityCandleRequestInFlight.set(key, request);
  }

  try {
    return await commodityCandleRequestInFlight.get(key);
  } catch (error) {
    const stale = getAnyCommodityCandleCacheEntry(ticker, interval);
    const staleCandles = stale && sanitizeCommodityCandleSeries(ticker, stale.data, interval);
    if (staleCandles && staleCandles.length > 0) {
      return { entry: stale, candles: staleCandles, cached: true, stale: true, error };
    }
    throw error;
  }
}

function queueCommodityTickerCandleWarmup(ticker, requestedInterval) {
  if (commodityTickerWarmupInFlight.has(ticker)) return;
  commodityTickerWarmupInFlight.add(ticker);

  setTimeout(async () => {
    try {
      const intervals = COMMODITY_CANDLE_WARMUP_INTERVALS.filter(interval => interval !== requestedInterval);
      // Limit concurrency to two requests so warmup never overwhelms Yahoo or
      // creates a large temporary memory spike.
      for (let index = 0; index < intervals.length; index += 2) {
        await Promise.allSettled(
          intervals.slice(index, index + 2).map(interval => getCommodityCandlesFast(ticker, interval))
        );
      }
    } finally {
      commodityTickerWarmupInFlight.delete(ticker);
    }
  }, 0);
}

const COMMODITY_CANDLE_INTERVAL_SECONDS = {
  "1min": 60,
  "5min": 5 * 60,
  "15min": 15 * 60,
  "30min": 30 * 60,
  "1h": 60 * 60,
  "1day": 24 * 60 * 60
};

function isCommodityMarketOpen(date = new Date()) {
  const parts = getEasternDateParts(date);
  const totalMin = parts.hour * 60 + parts.min;

  if (parts.wday === 6) return false; // Saturday
  if (parts.wday === 0) return totalMin >= 18 * 60; // Sunday evening open
  if (parts.wday === 5) return totalMin < 17 * 60; // Friday close

  // Monday-Thursday daily maintenance break from 5:00-6:00 PM ET.
  return totalMin < 17 * 60 || totalMin >= 18 * 60;
}

function isCommodityCandleSeriesFreshEnough(candles, interval) {
  const latest = candleSeriesLatestTimestamp(candles);
  if (!latest) return false;
  if (interval === "1day") return Date.now() - latest * 1000 <= 7 * 24 * 60 * 60 * 1000;

  const ageMs = Date.now() - latest * 1000;
  if (ageMs < 0) return true;

  if (isCommodityMarketOpen()) {
    const intervalMs = (COMMODITY_CANDLE_INTERVAL_SECONDS[interval] || 60) * 1000;
    // Free feeds may be delayed, but a previous-day intraday series is never valid.
    return ageMs <= Math.max(20 * 60 * 1000, intervalMs * 2 + 5 * 60 * 1000);
  }

  // Maintenance and weekend closures can legitimately leave the newest candle older.
  return ageMs <= 3 * 24 * 60 * 60 * 1000;
}

function normalizeCommodityEpochSeconds(value) {
  let numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) return null;

  // Some feeds return seconds, others milliseconds. Normalize both.
  while (numeric > 100000000000) numeric /= 1000;
  numeric = Math.floor(numeric);

  const nowSeconds = Math.floor(Date.now() / 1000);
  // Candle history can legitimately span months on 1h/1d charts. Only reject
  // impossible future values or data older than the longest supported history.
  if (numeric > nowSeconds + 5 * 60 || numeric < nowSeconds - 5 * 365 * 24 * 60 * 60) {
    return null;
  }

  return numeric;
}

function sanitizeCommodityCandleSeries(displayTicker, candles, interval) {
  if (!Array.isArray(candles)) return [];

  const nowSeconds = Math.floor(Date.now() / 1000);
  const byTimestamp = new Map();

  for (const candle of candles) {
    if (!candle || typeof candle !== "object") continue;

    const o = toNumber(candle.o ?? candle.open);
    const h = toNumber(candle.h ?? candle.high);
    const l = toNumber(candle.l ?? candle.low);
    const c = toNumber(candle.c ?? candle.close);

    // A few commodity feeds occasionally emit a placeholder bar containing a
    // zero OHLC field. One such bar forces the chart scale toward $0 and makes
    // every legitimate candle look flat. Never cache or return those bars.
    if (
      !commodityPriceIsValid(displayTicker, o) ||
      !commodityPriceIsValid(displayTicker, h) ||
      !commodityPriceIsValid(displayTicker, l) ||
      !commodityPriceIsValid(displayTicker, c)
    ) {
      continue;
    }

    let ts = normalizeCommodityEpochSeconds(candle.ts ?? candle.timestamp);
    if (!ts) ts = parseUtcCommodityTimestamp(candle.t ?? candle.datetime);
    if (!ts || ts > nowSeconds + 5 * 60) continue;

    // Repair harmless provider inconsistencies while preserving the actual
    // open and close. High must contain both, and low must contain both.
    const high = Math.max(h, o, c, l);
    const low = Math.min(l, o, c, h);

    byTimestamp.set(ts, {
      ...candle,
      t: candle.t || formatSyntheticStockCandleTime(ts * 1000, interval),
      ts,
      session: "commodity",
      o: roundCandleNumber(o),
      h: roundCandleNumber(high),
      l: roundCandleNumber(low),
      c: roundCandleNumber(c),
      v: Math.max(0, toNumber(candle.v ?? candle.volume) || 0)
    });
  }

  return Array.from(byTimestamp.values())
    .sort((a, b) => Number(a.ts || 0) - Number(b.ts || 0))
    .slice(-200);
}

function patchCommodityCandlesWithLivePrice(displayTicker, interval, candles) {
  const sanitized = sanitizeCommodityCandleSeries(displayTicker, candles, interval);
  if (sanitized.length === 0 || interval === "1day") return sanitized;
  if (!isCommodityMarketOpen()) return sanitized;

  const row = commodityPriceCache[displayTicker];
  if (!commodityRowIsFresh(row, 60 * 1000)) return sanitized;

  const livePrice = toNumber(row.price);
  if (!commodityPriceIsValid(displayTicker, livePrice)) return sanitized;

  const intervalSeconds = COMMODITY_CANDLE_INTERVAL_SECONDS[interval] || 60;
  const nowSeconds = Math.floor(Date.now() / 1000);
  const providerSeconds = normalizeCommodityEpochSeconds(row.lastUpdated);
  const receivedSeconds = Number(row.receivedAtMs) > 0
    ? Math.floor(Number(row.receivedAtMs) / 1000)
    : null;

  const quoteSeconds = Math.min(
    nowSeconds,
    Math.max(providerSeconds || 0, receivedSeconds || 0, nowSeconds - 5)
  );
  const bucketStart = Math.floor(quoteSeconds / intervalSeconds) * intervalSeconds;
  const output = sanitized.map(candle => ({ ...candle }));
  const latest = output[output.length - 1];
  const latestTs = normalizeCommodityEpochSeconds(latest && latest.ts) || 0;
  const latestBucket = Math.floor(latestTs / intervalSeconds) * intervalSeconds;

  if (latestBucket === bucketStart) {
    // sanitizeCommodityCandleSeries guarantees valid positive OHLC values here.
    latest.c = roundCandleNumber(livePrice);
    latest.h = roundCandleNumber(Math.max(Number(latest.h), Number(latest.o), livePrice));
    latest.l = roundCandleNumber(Math.min(Number(latest.l), Number(latest.o), livePrice));
    latest.liveQuotePatched = true;
    latest.quoteReceivedAt = nowSeconds;
    return output.slice(-200);
  }

  if (latestBucket < bucketStart) {
    output.push({
      t: formatSyntheticStockCandleTime(bucketStart * 1000, interval),
      ts: bucketStart,
      session: "commodity",
      o: roundCandleNumber(livePrice),
      h: roundCandleNumber(livePrice),
      l: roundCandleNumber(livePrice),
      c: roundCandleNumber(livePrice),
      v: 0,
      liveQuotePatched: true,
      quoteReceivedAt: nowSeconds
    });
  }

  return output.slice(-200);
}

app.get("/commodity/prices", async (req, res) => {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  const force = req.query.fresh === "1" || req.query.fresh === "true";
  await refreshCommodityQuotes(force);
  const prices = {};
  for (const ticker of COMMODITY_TICKERS) {
    if (commodityPriceCache[ticker]) prices[ticker] = commodityPriceCache[ticker];
  }
  res.json({
    success: Object.keys(prices).length > 0,
    prices,
    source: "Yahoo Finance commodity futures",
    updatedAt: Math.floor(Date.now() / 1000),
    streamHealthy: commodityWsLastMessageAt > 0 && Date.now() - commodityWsLastMessageAt <= COMMODITY_WS_STALE_MS
  });
});

app.get("/commodity/price", async (req, res) => {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  const ticker = normalizeCommodityTicker(req.query.ticker);
  if (!isCommodityTicker(ticker)) return res.json({ error: "Unknown commodity ticker." });
  await refreshCommodityQuotes(req.query.fresh === "1" || !commodityRowIsFresh(commodityPriceCache[ticker]));
  res.json(commodityPriceCache[ticker] || { ticker, error: "No commodity price available." });
});

app.get("/commodity/candles", async (req, res) => {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");

  const ticker = normalizeCommodityTicker(req.query.ticker);
  const requestedInterval = String(req.query.interval || "1m");
  const intervalMap = { "1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min", "1h": "1h", "1d": "1day" };
  const interval = intervalMap[requestedInterval] || requestedInterval;
  if (!isCommodityTicker(ticker)) return res.json({ ticker, error: "Unknown commodity ticker." });
  if (!Object.values(intervalMap).includes(interval)) return res.json({ ticker, error: "Unsupported commodity interval." });

  // Never make chart loading wait for a separate quote request. The chart can
  // render from futures candles immediately; a quote refresh runs alongside it.
  if (!commodityRowIsFresh(commodityPriceCache[ticker], 5 * 1000)) {
    refreshCommodityQuotes(false).catch(error =>
      console.warn(`[COMMODITY] Background quote refresh failed: ${error.message}`)
    );
  }

  try {
    const result = await getCommodityCandlesFast(ticker, interval);
    const patched = patchCommodityCandlesWithLivePrice(ticker, interval, result.candles);
    if (!patched.length) throw new Error("Commodity provider returned no valid candles.");

    queueCommodityTickerCandleWarmup(ticker, interval);

    return res.json({
      ticker,
      interval,
      candles: withChartIndicators(patched),
      cached: result.cached === true,
      stale: result.stale === true,
      commodity: true,
      source: result.entry?.source || "Yahoo Finance commodity futures",
      liveQuotePatched: patched.some(candle => candle.liveQuotePatched === true),
      livePrice: commodityPriceCache[ticker] && commodityPriceCache[ticker].price,
      livePriceTimestamp: commodityPriceCache[ticker] && commodityPriceCache[ticker].lastUpdated,
      extendedHoursIncluded: true,
      indicators: { rsiPeriod: RSI_PERIOD, rsiSource: "candle-close" }
    });
  } catch (error) {
    console.warn(`[COMMODITY] ${ticker} ${interval} candles failed: ${error.message}`);
    return res.json({
      ticker,
      interval,
      commodity: true,
      error: "Commodity chart provider did not return usable data.",
      providerError: error.message
    });
  }
});

app.get("/commodity/debug", async (_req, res) => {
  await refreshCommodityQuotes(false);
  res.json({
    tickers: COMMODITY_TICKERS,
    prices: commodityPriceCache,
    resolvedTwelveSymbols: commodityResolvedTwelveSymbol,
    websocketReadyState: commodityWs ? commodityWs.readyState : null,
    websocketLastMessageAgeMs: commodityWsLastMessageAt > 0 ? Date.now() - commodityWsLastMessageAt : null
  });
});

app.get("/prices", async (req, res) => {
  const wantsFresh = req.query.fresh === "1" || req.query.fresh === "true";

  applySyntheticProjectStockQuotes();

  const cacheEmpty = Object.keys(priceCache).length === 0;
  const cacheHasAllTickers = TICKERS.every(ticker => {
    const row = priceCache[ticker];
    return row && Number(row.price) > 0;
  });

  if (ENABLE_YAHOO_ON_DEMAND_QUOTES) {
    if (wantsFresh || cacheEmpty || !cacheHasAllTickers) {
      try {
        await refreshAllYahooQuotes();
      } catch (e) {
        console.error("[YAHOO] /prices refresh failed", e.message);
      }
    } else {
      triggerAllYahooQuoteRefresh();
    }
  }

  applySyntheticProjectStockQuotes();

  res.json(priceCache);
});

app.get("/price", async (req, res) => {
  const ticker = String(req.query.ticker || "").toUpperCase();
  const wantsFresh = req.query.fresh === "1" || req.query.fresh === "true";

  let data = priceCache[ticker];

  if (!isRealStockTicker(ticker)) {
    return res.json({
      ticker,
      error: "Unknown stock ticker. Stock prices must come from real market data; synthetic fallback is disabled."
    });
  }

  const ageMs = data && data.fetchedAt ? Date.now() - data.fetchedAt : Infinity;

  if (ENABLE_YAHOO_ON_DEMAND_QUOTES) {
    if (!data || wantsFresh || ageMs > PRICE_CACHE_MAX_STALE_MS) {
      try {
        await fetchYahooQuotesDeduped([ticker]);
        data = priceCache[ticker];
      } catch (_) {}

      if (!data || !Number(data.price) || Number(data.price) <= 0) {
        try {
          await fetchYahooChartQuoteFallback(ticker);
          data = priceCache[ticker];
        } catch (e) {
          console.warn(`[YAHOO] /price fallback failed for ${ticker}: ${e.message}`);
        }
      }
    } else if (ageMs > PRICE_CACHE_TTL_MS) {
      triggerYahooQuoteRefresh([ticker]);
    }
  }

  res.json(
    data
      ? {
          ticker,
          ...data
        }
      : {
          error: "No data"
        }
  );
});

app.get("/candles", async (req, res) => {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate");
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");

  const ticker = String(req.query.ticker || "").toUpperCase();
  const interval = req.query.interval || "1min";

  const intervalMap = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "1d": "1day"
  };

  const tdInterval = intervalMap[interval] || interval;
  const outputsize = 200;

  if (!ticker) {
    return res.json({ error: "Missing ticker." });
  }

  if (!isRealStockTicker(ticker)) {
    return res.json({
      ticker,
      interval: tdInterval,
      error: "Unknown stock ticker. Stock candles must come from real market data; synthetic fallback is disabled.",
      synthetic: false
    });
  }

  const respondWithCandles = (candles, source, cached = false, extra = {}) => {
    const cleaned = repairIsolatedStockWicks(candles, tdInterval);
    const repairedCount = cleaned.reduce(
      (count, candle) => count + (candle && candle.badTickRepaired ? 1 : 0),
      0
    );

    if (!cached) {
      setCachedCandles(ticker, tdInterval, cleaned, source);
    }

    return res.json({
      ticker,
      interval: tdInterval,
      candles: withChartIndicators(cleaned),
      cached,
      indicators: { rsiPeriod: RSI_PERIOD, rsiSource: "candle-close" },
      livePatched: false,
      synthetic: false,
      source,
      repairedCount,
      extendedHoursIncluded: tdInterval !== "1day",
      ...extra
    });
  };

  const cachedEntry = getCachedCandleEntry(ticker, tdInterval);
  const cachedSourceName = String(cachedEntry && cachedEntry.source || "").toLowerCase();
  const cachedProviderIsAllowed =
    tdInterval === "1day" ||
    (!cachedSourceName.includes("massive") && !cachedSourceName.includes("historical"));

  if (
    cachedEntry &&
    cachedProviderIsAllowed &&
    Array.isArray(cachedEntry.data) &&
    cachedEntry.data.length > 0 &&
    (tdInterval === "1day" || isStockCandleSeriesFreshEnough(cachedEntry.data, tdInterval))
  ) {
    triggerYahooQuoteRefresh([ticker]);
    return respondWithCandles(cachedEntry.data, cachedEntry.source || "Cached provider", true);
  }

  // Preserve the last real provider series while refreshing. If Yahoo or Twelve
  // Data is temporarily unavailable, players should still see the last legitimate
  // chart instead of an empty "unavailable" panel.
  const providerErrors = {};

  // Yahoo is authoritative for intraday stock charts because it includes the
  // complete pre-market, regular, and after-hours session in one real series.
  try {
    const yahooCandles = await fetchYahooStockCandlesDeduped(ticker, tdInterval, outputsize);

    if (tdInterval !== "1day" && !isStockCandleSeriesFreshEnough(yahooCandles, tdInterval)) {
      throw new Error(
        `Yahoo response ended on ${candleSeriesLatestEasternDateKey(yahooCandles) || "an unknown date"}; ` +
        `expected ${expectedLatestStockCandleDateKey()}.`
      );
    }

    return respondWithCandles(yahooCandles, "Yahoo Finance primary");
  } catch (error) {
    providerErrors.yahoo = error && error.message || String(error);
  }

  // Twelve Data is the first fallback. The previous order preferred Massive,
  // which is where the isolated $10-$15 false bodies were still entering the
  // chart whenever Yahoo was temporarily unavailable.
  if (
    TWELVE_DATA_API_KEY &&
    shouldUseTwelveDataCandleBackup(ticker) &&
    twelveDataCandleCreditsUsedToday < MAX_TWELVE_DATA_CANDLE_CREDITS_PER_DAY
  ) {
    const realTicker = getRealTicker(ticker);
    const url =
      `https://api.twelvedata.com/time_series` +
      `?symbol=${encodeURIComponent(realTicker)}` +
      `&interval=${encodeURIComponent(tdInterval)}` +
      `&outputsize=${outputsize}` +
      `&apikey=${encodeURIComponent(TWELVE_DATA_API_KEY)}`;

    try {
      const data = await tdRequest(url, TD_PRIORITY.candle);
      if (data.status === "error" || !Array.isArray(data.values)) {
        throw new Error(data.message || "No Twelve Data candle data.");
      }

      const twelveCandles = data.values
        .slice()
        .reverse()
        .map(value => normalizeStockCandleRecord({
          datetime: value.datetime,
          interval: tdInterval,
          open: parseFloat(value.open),
          high: parseFloat(value.high),
          low: parseFloat(value.low),
          close: parseFloat(value.close),
          volume: parseFloat(value.volume)
        }))
        .filter(Boolean);

      if (twelveCandles.length === 0) {
        throw new Error("Twelve Data returned no usable stock candles.");
      }

      if (tdInterval !== "1day" && !isStockCandleSeriesFreshEnough(twelveCandles, tdInterval)) {
        throw new Error("Twelve Data candle response was stale for the active session.");
      }

      twelveDataCandleCreditsUsedToday++;
      return respondWithCandles(twelveCandles, "Twelve Data fallback");
    } catch (error) {
      providerErrors.twelveData = error && error.message || String(error);
    }
  }

  // Accuracy is preferred over fabricated availability. Massive remains disabled
  // for intraday charts because its malformed aggregate rows caused false drops.
  // However, a previously cached Yahoo/Twelve Data series is still real market data,
  // so return that last available series rather than blanking every stock chart.
  if (
    cachedEntry &&
    cachedProviderIsAllowed &&
    Array.isArray(cachedEntry.data) &&
    cachedEntry.data.length > 0
  ) {
    return respondWithCandles(
      cachedEntry.data,
      `${cachedEntry.source || "Real market provider"} (last available)`,
      true,
      {
        stale: true,
        providerErrors
      }
    );
  }

  return res.json({
    ticker,
    interval: tdInterval,
    error: "No accurate stock candle provider returned usable data.",
    providerErrors,
    synthetic: false
  });
});

// ============================
// Start
// ============================
async function start() {
  await seedPrevClose();

  // Keep the stock list warm before players ask Roblox for prices.
  // This avoids players joining into a mostly-$0.00 list while the cache is still being filled.
  if (process.env.WARM_YAHOO_QUOTES !== "false") {
    warmYahooQuotes()
      .then(updated => console.log(`[YAHOO] Warmed ${updated} quotes on startup`))
      .catch(err => console.error("[YAHOO] Startup warm failed", err.message));
  }

  if (ENABLE_TWELVE_DATA_POLLING && isExtendedHours()) {
    console.log("[INIT] Extended hours detected, running initial Twelve Data poll...");

    for (let i = 0; i < Math.ceil(TICKERS.length / BATCH_SIZE); i++) {
      await pollTwelveDataBatch();
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }

  connectFinnhub();

  // Commodity quotes and charts use the same Yahoo futures instruments so the
  // displayed price and every timeframe remain aligned.
  refreshCommodityQuotes(true)
    .then(() => {
      console.log(`[COMMODITY] Startup quote cache ready: ${Object.keys(commodityPriceCache).length}/${COMMODITY_TICKERS.length}`);
      COMMODITY_TICKERS.forEach(ticker => queueCommodityTickerCandleWarmup(ticker, null));
    })
    .catch(error => console.warn(`[COMMODITY] Startup refresh failed: ${error.message}`));

  // Crypto prices use a one-second Binance ticker stream, with Binance REST,
  // FreeCryptoAPI and CoinGecko as automatic fallbacks.
  connectCryptoTickerStream();
  startCryptoPriceWatchdog();
  fetchCryptoPrices(CRYPTO_SYMBOLS, { forceRest: true })
    .then(result => console.log(`[CRYPTO] Startup cache ready: ${Object.keys(result.prices || {}).length}/${CRYPTO_SYMBOLS.length}`))
    .catch(err => console.warn(`[CRYPTO] Startup refresh failed: ${err.message}`));

  startFinnhubRestPolling();
  startYahooQuotePolling();

  if (ENABLE_TWELVE_DATA_POLLING) {
    startTwelveDataPolling();
  } else {
    console.log("[12DATA] Quote polling disabled. Set ENABLE_TWELVE_DATA_POLLING=true to re-enable it.");
  }

  app.listen(PORT, () => {
    console.log(`[SERVER] Ready on ${PORT}`);
  });
}

start();
