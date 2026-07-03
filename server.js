const express = require("express");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 8080;

app.use(express.json({ limit: "25kb" }));

const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
const TWELVE_DATA_API_KEY = process.env.TWELVE_DATA_API_KEY;
const FREECRYPTO_API_KEY = process.env.FREECRYPTO_API_KEY;
const FREECRYPTO_BASE_URL = "https://api.freecryptoapi.com/v1";

const priceCache = {};

const TICKERS = [
  "ORNG", "MHRD", "MVDO", "AMZG", "ELPHT", "DATA", "NKLA", "SKYX", "BKSG", "HCC", "ELLY", "PMK", "M", "FMT", "DVS", "WXM",
  "ABMD", "NFKS", "BUM", "DGBE", "REVL", "MNEY", "VKNEE", "BEAR", "NICY", "PPL", "INFO", "OVER", "WBAB", "SMNY", "BC",
  "CHHD", "VSS",
  "MASK", "MNTS", "DSY", "INHD", "CLDI", "AZI", "DXST", "WCT", "AIXI", "CODX", "GOVX", "CHAI", "CDLX", "DCX", "CLPR"
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
  CHHD: "SCHD",
  VSS: "VOO",
  MASK: "MASK",
  MNTS: "MNTS",
  DSY: "DSY",
  INHD: "INHD",
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
const YAHOO_FLOAT_METADATA_TTL_MS = Number(process.env.YAHOO_FLOAT_METADATA_TTL_MS || 6 * 60 * 60 * 1000);
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
  const floatShares = firstPositiveNumber(
    metadata.floatShares,
    metadata.publicFloat,
    metadata.sharesOutstanding,
    existing.floatShares,
    existing.publicFloat
  );
  const sharesOutstanding = firstPositiveNumber(
    metadata.sharesOutstanding,
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

async function fetchYahooFloatMetadata(displayTicker) {
  const ticker = String(displayTicker || "").toUpperCase();
  if (!ticker || !isRealStockTicker(ticker)) return false;

  const cached = getCachedFloatMetadata(ticker);
  if (cached) {
    applyFloatMetadataToPriceCache(ticker, cached);
    return true;
  }

  const yahooSymbol = yahooTickerSymbol(getRealTicker(ticker));
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

  return applyFloatMetadataToPriceCache(ticker, metadata);
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
  const sharesOutstanding = firstPositiveNumber(row.sharesOutstanding, existing && existing.sharesOutstanding);
  const floatShares = firstPositiveNumber(row.floatShares, row.sharesFloat, row.freeFloat, row.sharesOutstanding, existing && existing.floatShares);
  const publicFloat = firstPositiveNumber(floatShares, existing && existing.publicFloat);

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
  return ENABLE_TWELVE_DATA_MARKET_CANDLE_BACKUP && isRegularMarketHours();
}

function getCachedCandles(ticker, interval) {
  const key = `${ticker}:${interval}`;
  const entry = candleCache[key];

  if (!entry) return null;

  const age = Date.now() - entry.fetchedAt;

  if (age > getCandleTTL(interval)) return null;

  return entry.data;
}

function setCachedCandles(ticker, interval, data) {
  const key = `${ticker}:${interval}`;

  candleCache[key] = {
    data,
    fetchedAt: Date.now()
  };
}

const stockCandleInFlight = new Map();

async function fetchYahooStockCandlesDeduped(ticker, interval, limit) {
  const key = `${ticker}:${interval}:${limit}`;

  if (stockCandleInFlight.has(key)) {
    return stockCandleInFlight.get(key);
  }

  const promise = fetchYahooStockCandles(ticker, interval, limit)
    .then(candles => {
      setCachedCandles(ticker, interval, candles);
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
  if (!Array.isArray(candles) || candles.length === 0) return candles;

  const seconds = STOCK_CANDLE_INTERVAL_SECONDS[interval];
  if (!seconds || interval === "1day") return candles;

  const info = priceCache[ticker];
  const livePrice = info && toNumber(info.price);

  if (!livePrice || livePrice <= 0) return candles;

  const stepMs = seconds * 1000;
  const nowBucketMs = Math.floor(Date.now() / stepMs) * stepMs;
  const out = candles.map(c => ({ ...c }));
  const last = out[out.length - 1];
  const lastMsRaw = parseStockCandleUtcMs(last.t);

  if (lastMsRaw === null) {
    const prevClose = toNumber(last.c) || livePrice;
    out[out.length - 1] = makeLiveSyntheticCandle(ticker, interval, nowBucketMs, prevClose, livePrice, 1);
    return out;
  }

  const lastBucketMs = Math.floor(lastMsRaw / stepMs) * stepMs;

  if (lastBucketMs === nowBucketMs) {
    const open = toNumber(last.o) || toNumber(last.c) || livePrice;
    const high = Math.max(toNumber(last.h) || open, open, livePrice);
    const low = Math.max(0.000001, Math.min(toNumber(last.l) || open, open, livePrice));

    last.h = roundCandleNumber(high);
    last.l = roundCandleNumber(low);
    last.c = roundCandleNumber(livePrice);
    return out;
  }

  if (lastBucketMs > nowBucketMs) {
    return out;
  }

  const missingBuckets = Math.min(30, Math.floor((nowBucketMs - lastBucketMs) / stepMs));
  let previousClose = toNumber(last.c) || livePrice;

  for (let i = 1; i <= missingBuckets; i++) {
    const tMs = lastBucketMs + i * stepMs;
    const progress = i / missingBuckets;
    let close = previousClose + ((livePrice - previousClose) * progress);

    if (i < missingBuckets) {
      const hash = stableHashString(`${ticker}:${interval}:${tMs}`);
      const wiggle = (deterministicUnit(hash) - 0.5) * Math.max(livePrice * 0.002, 0.000001);
      close += wiggle;
    } else {
      close = livePrice;
    }

    const candle = makeLiveSyntheticCandle(ticker, interval, tMs, previousClose, close, i);
    out.push(candle);
    previousClose = close;

    while (out.length > candles.length) {
      out.shift();
    }
  }

  return out;
}

const YAHOO_INTERVALS = {
  "1min": { interval: "1m", range: "1d" },
  "5min": { interval: "5m", range: "5d" },
  "15min": { interval: "15m", range: "5d" },
  "30min": { interval: "30m", range: "1mo" },
  "1h": { interval: "60m", range: "1mo" },
  "1day": { interval: "1d", range: "1y" }
};

function yahooTickerSymbol(ticker) {
  return getRealTicker(ticker).replace(".", "-");
}

async function fetchYahooStockCandles(ticker, interval, limit = 200) {
  const cfg = YAHOO_INTERVALS[interval];

  if (!cfg) {
    throw new Error("Unsupported Yahoo stock candle interval.");
  }

  const yahooSymbol = yahooTickerSymbol(ticker);
  const url =
    `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(yahooSymbol)}` +
    `?range=${encodeURIComponent(cfg.range)}` +
    `&interval=${encodeURIComponent(cfg.interval)}` +
    `&includePrePost=false`;

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
    throw new Error(resp.data?.chart?.error?.description || `Yahoo HTTP ${resp.status}`);
  }

  const chart = resp.data && resp.data.chart;
  const result = chart && Array.isArray(chart.result) && chart.result[0];

  if (!result) {
    throw new Error(chart?.error?.description || "No Yahoo chart result returned.");
  }

  const timestamps = Array.isArray(result.timestamp) ? result.timestamp : [];
  const quote = result.indicators && result.indicators.quote && result.indicators.quote[0];

  if (!quote || timestamps.length === 0) {
    throw new Error("Yahoo chart result missing quote data.");
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

    candles.push({
      t: formatSyntheticStockCandleTime(Number(timestamps[i]) * 1000, interval),
      o: roundCandleNumber(o),
      h: roundCandleNumber(h),
      l: roundCandleNumber(l),
      c: roundCandleNumber(c),
      v
    });
  }

  if (candles.length === 0) {
    throw new Error("No usable Yahoo candle data returned.");
  }

  return candles.slice(-limit);
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

const cryptoPriceCache = {};

let cryptoCacheFetchedAt = 0;

const CRYPTO_CACHE_TTL_MS = 4500;

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
    lastUpdated: Math.floor(Date.now() / 1000),
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
        normalizeManyCryptoPrices(batch.data, symbols, "FreeCryptoAPI")
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
        "FreeCryptoAPI"
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

      prices[symbol] = {
        symbol,
        name: CRYPTO_NAMES[symbol] || symbol,
        price,
        change24h: toNumber(row.usd_24h_change),
        marketCap: toNumber(row.usd_market_cap),
        volume24h: toNumber(row.usd_24h_vol),
        lastUpdated: Math.floor(Date.now() / 1000),
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

async function fetchCryptoPrices(symbols = CRYPTO_SYMBOLS) {
  const requestedSymbols = symbols
    .map(normalizeSymbol)
    .filter(symbol => CRYPTO_SYMBOLS.includes(symbol));

  const now = Date.now();

  const cacheHasAllRequested = requestedSymbols.every(symbol => {
    return cryptoPriceCache[symbol];
  });

  if (
    cacheHasAllRequested &&
    now - cryptoCacheFetchedAt < CRYPTO_CACHE_TTL_MS
  ) {
    return {
      prices: cryptoPriceCache,
      cached: true
    };
  }

  const freeCrypto = await fetchFreeCryptoAPI(requestedSymbols);

  let normalized = {
    ...freeCrypto.prices
  };

  let providerError = freeCrypto.error || null;

  const missing = requestedSymbols.filter(symbol => {
    return !normalized[symbol];
  });

  if (missing.length > 0) {
    const fallback = await fetchCoinGeckoFallback(missing);

    Object.assign(normalized, fallback.prices);

    if (fallback.error && Object.keys(normalized).length === 0) {
      providerError = providerError
        ? `${providerError}; ${fallback.error}`
        : fallback.error;
    }
  }

  if (Object.keys(normalized).length === 0) {
    return {
      error:
        providerError ||
        "No usable crypto prices returned by FreeCryptoAPI or fallback provider."
    };
  }

  for (const [symbol, info] of Object.entries(normalized)) {
    cryptoPriceCache[symbol] = info;
  }

  cryptoCacheFetchedAt = now;

  return {
    prices: cryptoPriceCache,
    cached: false,
    providerError
  };
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
      candles: cached.data,
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
      candles,
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
      candles,
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

const ROBLOX_GROUP_ID = String(process.env.ROBLOX_GROUP_ID || "15696460");
const ROBLOX_OPEN_CLOUD_API_KEY = process.env.ROBLOX_OPEN_CLOUD_API_KEY || "";
const GROUP_SYNC_SECRET = process.env.GROUP_SYNC_SECRET || "";

const GAME_ROLE_TO_GROUP_ROLE_NAME = {
  "Intern Trader": process.env.GROUP_ROLE_INTERN_NAME || "Intern Trader",
  "Rookie Trader": process.env.GROUP_ROLE_ROOKIE_NAME || "Rookie Trader",
  "Intermediate Trader": process.env.GROUP_ROLE_INTERMEDIATE_NAME || "Intermediate Trader",
  "Day Trader": process.env.GROUP_ROLE_DAY_TRADER_NAME || "Day Trader"
};

let cachedGroupRolesByDisplayName = null;
let cachedGroupRolesFetchedAtMs = 0;
const GROUP_ROLE_CACHE_TTL_MS = 10 * 60 * 1000;

function normalizeGameRole(role) {
  role = String(role || "").trim();
  return Object.prototype.hasOwnProperty.call(GAME_ROLE_TO_GROUP_ROLE_NAME, role) ? role : "";
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
  const url =
    `https://apis.roblox.com/cloud/v2/groups/${encodeURIComponent(ROBLOX_GROUP_ID)}` +
    `/memberships/${encodeURIComponent(String(userId))}`;

  return robloxOpenCloudJson(url, {
    method: "PATCH",
    body: JSON.stringify({
      role: groupRoleResource
    })
  });
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

app.post("/group-role/sync", async (req, res) => {
  if (!assertGroupSyncSecret(req)) {
    return res.status(401).json({ ok: false, error: "Unauthorized." });
  }

  const userId = Number(req.body && req.body.userId);
  const username = String(req.body && req.body.username || "");
  const gameRole = normalizeGameRole(req.body && req.body.role);
  const inGroup = req.body && req.body.inGroup === true;

  if (!Number.isInteger(userId) || userId <= 0) {
    return res.status(400).json({ ok: false, error: "Invalid userId." });
  }

  if (!gameRole) {
    return res.status(400).json({
      ok: false,
      error: "Invalid game role.",
      allowedRoles: Object.keys(GAME_ROLE_TO_GROUP_ROLE_NAME)
    });
  }

  if (!inGroup) {
    return res.json({
      ok: true,
      skipped: true,
      reason: "Player is not in group.",
      userId,
      username,
      gameRole
    });
  }

  try {
    let roles = await getGroupRolesByDisplayName(false);
    const desiredGroupRoleName = GAME_ROLE_TO_GROUP_ROLE_NAME[gameRole];
    let desiredGroupRole = roles[desiredGroupRoleName];

    // If a role was just renamed/created, refresh once before failing.
    if (!desiredGroupRole) {
      roles = await getGroupRolesByDisplayName(true);
      desiredGroupRole = roles[desiredGroupRoleName];
    }

    if (!desiredGroupRole) {
      return res.status(400).json({
        ok: false,
        error: `No group role named "${desiredGroupRoleName}" was found in group ${ROBLOX_GROUP_ID}.`,
        gameRole,
        wantedGroupRoleName: desiredGroupRoleName,
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
      groupRole: desiredGroupRole.displayName,
      groupRoleResource: desiredGroupRole.resource,
      roblox: robloxResult
    });
  } catch (e) {
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
  const symbol = normalizeSymbol(req.query.symbol || "BTC");

  if (!CRYPTO_SYMBOLS.includes(symbol)) {
    return res.json({
      error: "Unsupported debug symbol."
    });
  }

  if (!FREECRYPTO_API_KEY) {
    return res.json({
      freeCryptoApiKeyPresent: false,
      error: "FREECRYPTO_API_KEY is not set."
    });
  }

  const url = `${FREECRYPTO_BASE_URL}/getData?symbol=${encodeURIComponent(symbol)}`;

  const raw = await fetchJsonWithTimeout(url, {
    headers: {
      Authorization: `Bearer ${FREECRYPTO_API_KEY}`,
      Accept: "application/json"
    }
  }).catch(e => ({
    ok: false,
    status: 0,
    data: {
      error: e.message
    }
  }));

  res.json({
    freeCryptoApiKeyPresent: true,
    status: raw.status,
    ok: raw.ok,
    normalized: normalizeCryptoPriceFromPayload(
      raw.data,
      symbol,
      "FreeCryptoAPI"
    ),
    raw: raw.data
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
    return res.json({
      error: "Missing ticker."
    });
  }

  const realStockTicker = isRealStockTicker(ticker);

  if (!realStockTicker) {
    return res.json({
      ticker,
      interval: tdInterval,
      error: "Unknown stock ticker. Stock candles must come from real market data; synthetic fallback is disabled.",
      synthetic: false
    });
  }

  const cached = getCachedCandles(ticker, tdInterval);

  if (cached) {
    triggerYahooQuoteRefresh([ticker]);

    return res.json({
      ticker,
      interval: tdInterval,
      candles: patchStockCandlesWithLivePrice(ticker, tdInterval, cached),
      cached: true,
      livePatched: true
    });
  }

  // Primary stock chart path: Yahoo Finance chart data. This avoids burning
  // Twelve Data API credits just from players opening charts. Pre-market and
  // after-hours candles are excluded to reduce sparse intraday chart gaps.
  // Timestamps are formatted in UTC; the Roblox LocalScript displays them as ET.
  try {
    const yahooCandles = await fetchYahooStockCandlesDeduped(ticker, tdInterval, outputsize);

    return res.json({
      ticker,
      interval: tdInterval,
      candles: patchStockCandlesWithLivePrice(ticker, tdInterval, yahooCandles),
      cached: false,
      livePatched: true,
      synthetic: false,
      source: "Yahoo Finance"
    });
  } catch (yahooErr) {
    // Real stocks use Twelve Data as a backup when Yahoo fails. If the backup is disabled for
    // this request/session, return unavailable instead of fabricating candles.
    if (!shouldUseTwelveDataCandleBackup(ticker)) {
      return res.json({
        ticker,
        interval: tdInterval,
        error: "Yahoo did not return usable stock candles, and Twelve Data backup is disabled for this request/session.",
        providerError: yahooErr.message,
        synthetic: false
      });
    }
  }

  if (!TWELVE_DATA_API_KEY) {
    return res.json({
      ticker,
      interval: tdInterval,
      error: "Yahoo did not return usable stock candles, and TWELVE_DATA_API_KEY is not set for real-data backup.",
      synthetic: false
    });
  }

  if (twelveDataCandleCreditsUsedToday >= MAX_TWELVE_DATA_CANDLE_CREDITS_PER_DAY) {
    return res.json({
      ticker,
      interval: tdInterval,
      error: "Proxy Twelve Data candle cap reached for today. Real-data candle backup unavailable.",
      synthetic: false
    });
  }

  const realTicker = getRealTicker(ticker);
  const url =
    `https://api.twelvedata.com/time_series` +
    `?symbol=${realTicker}` +
    `&interval=${tdInterval}` +
    `&outputsize=${outputsize}` +
    `&apikey=${TWELVE_DATA_API_KEY}`;

  try {
    const data = await tdRequest(url, TD_PRIORITY.candle);

    if (data.status === "error" || !data.values) {
      return res.json({
        ticker,
        interval: tdInterval,
        error: data.message || "No Twelve Data candle data",
        synthetic: false
      });
    }

    const candles = data.values.reverse().map(v => ({
      t: v.datetime,
      o: parseFloat(v.open),
      h: parseFloat(v.high),
      l: parseFloat(v.low),
      c: parseFloat(v.close),
      v: parseFloat(v.volume)
    }));

    setCachedCandles(ticker, tdInterval, candles);
    twelveDataCandleCreditsUsedToday++;

    res.json({
      ticker,
      interval: tdInterval,
      candles: patchStockCandlesWithLivePrice(ticker, tdInterval, candles),
      cached: false,
      livePatched: true,
      synthetic: false,
      source: "Twelve Data"
    });
  } catch (e) {
    res.json({
      ticker,
      interval: tdInterval,
      error: e.message,
      synthetic: false
    });
  }
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
