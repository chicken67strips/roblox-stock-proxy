const express = require("express");
const WebSocket = require("ws");
const app = express();
const PORT = process.env.PORT || 8080;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
const TWELVE_DATA_API_KEY = process.env.TWELVE_DATA_API_KEY;

const priceCache = {};
const TICKERS = [
  "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK.B", "AVGO", "LLY", "JPM", "V", "WMT", "UNH", "XOM",
  "AMD", "NFLX", "CRM", "ADBE", "ORCL", "COST", "DIS", "BA", "NKE", "PYPL", "INTC", "UBER", "ABNB", "SBUX", "KO",
  "MASK", "MNTS", "DSY", "INHD", "CLDI", "AZI", "DXST", "WCT", "AIXI", "CODX", "GOVX", "CHAI", "CDLX", "DCX", "CLPR"
];

let wsReady = false;
let wsInstance = null;
let lastWsTradeTime = 0;
const WS_QUIET_THRESHOLD_MS = 60000;

// ============================
// Market hours check (UTC)
// Regular hours: Mon-Fri 14:30-21:00 UTC (9:30 AM - 4:00 PM ET)
// Extended hours: Mon-Fri 8:00-23:00 UTC (4:00 AM - 7:00 PM ET)
// ============================
function getUTCComponents() {
  const now = new Date();
  return {
    wday: now.getUTCDay(), // 0=Sun, 1=Mon ... 6=Sat
    hour: now.getUTCHours(),
    min: now.getUTCMinutes()
  };
}

function isWeekend() {
  const { wday } = getUTCComponents();
  return wday === 0 || wday === 6;
}

function isRegularMarketHours() {
  if (isWeekend()) return false;
  const { hour, min } = getUTCComponents();
  const totalMin = hour * 60 + min;
  return totalMin >= (14 * 60 + 30) && totalMin < (21 * 60);
}

function isExtendedHours() {
  if (isWeekend()) return false;
  const { hour, min } = getUTCComponents();
  const totalMin = hour * 60 + min;
  // Pre-market: 4am-9:30am ET = 8:00-14:30 UTC
  // After-hours: 4pm-7pm ET = 20:00-23:00 UTC
  return (totalMin >= (8 * 60) && totalMin < (14 * 60 + 30)) ||
         (totalMin >= (20 * 60) && totalMin < (23 * 60));
}

// ============================
// Seed from Finnhub on startup
// ============================
async function seedPrevClose() {
  console.log("[SEED] Starting...");
  for (const ticker of TICKERS) {
    try {
      const res = await fetch(`https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${FINNHUB_API_KEY}`);
      const data = await res.json();
      if (data.c || data.pc) {
        priceCache[ticker] = {
          price: data.c || data.pc,
          prevClose: data.pc || data.c,
          changePct: data.dp ? data.dp.toFixed(2) : "0.00"
        };
        console.log(`[SEED] ${ticker} = $${priceCache[ticker].price}`);
      }
    } catch (e) {
      console.error(`[SEED] ${ticker} failed`, e.message);
    }
  }
}

// ============================
// Twelve Data polling (extended hours)
// Batches of 8 tickers per call to stay under free tier limits
// Only runs during extended hours when Finnhub WS is quiet
// ============================
const BATCH_SIZE = 8;
let twelveDataBatchIndex = 0;
let twelveDataCreditsUsedToday = 0;
let lastCreditReset = new Date().toDateString();
const MAX_CREDITS_PER_DAY = 750; // Leave 50 buffer below 800 limit

function resetCreditsIfNewDay() {
  const today = new Date().toDateString();
  if (today !== lastCreditReset) {
    twelveDataCreditsUsedToday = 0;
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
  twelveDataBatchIndex = (twelveDataBatchIndex + 1) % Math.ceil(TICKERS.length / BATCH_SIZE);

  if (batch.length === 0) return;

  try {
    const symbols = batch.join(",");
    const url = `https://api.twelvedata.com/quote?symbol=${symbols}&apikey=${TWELVE_DATA_API_KEY}`;
    const res = await fetch(url);
    const data = await res.json();

    // Response is either a single object (1 ticker) or a dict keyed by ticker
    const results = batch.length === 1 ? { [batch[0]]: data } : data;

    let updated = 0;
    for (const ticker of batch) {
      const quote = results[ticker];
      if (!quote || quote.status === "error" || !quote.close) continue;

      const price = parseFloat(quote.close);
      const prevClose = parseFloat(quote.previous_close) || (priceCache[ticker] && priceCache[ticker].prevClose) || price;
      const changePct = prevClose ? (((price - prevClose) / prevClose) * 100).toFixed(2) : "0.00";

      if (!isNaN(price) && price > 0) {
        priceCache[ticker] = { price, prevClose, changePct };
        updated++;
      }
    }

    twelveDataCreditsUsedToday += batch.length;
    console.log(`[12DATA] Updated ${updated}/${batch.length} tickers (batch ${twelveDataBatchIndex}/${Math.ceil(TICKERS.length / BATCH_SIZE)}) | Credits used today: ${twelveDataCreditsUsedToday}`);
  } catch (e) {
    console.error("[12DATA] Batch poll failed", e.message);
  }
}

// ============================
// Finnhub WebSocket (regular hours live trades)
// ============================
function handleTradeMessage(msg) {
  if (msg.type !== "trade" || !Array.isArray(msg.data)) return;
  lastWsTradeTime = Date.now();
  for (const trade of msg.data) {
    const ticker = trade.s;
    const price = trade.p;
    if (!ticker || typeof price !== "number") continue;
    const existing = priceCache[ticker];
    const prevClose = existing && existing.prevClose ? existing.prevClose : price;
    const changePct = prevClose ? (((price - prevClose) / prevClose) * 100).toFixed(2) : "0.00";
    priceCache[ticker] = { price, prevClose, changePct };
  }
}

function connectFinnhub() {
  const ws = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_API_KEY}`);
  wsInstance = ws;
  ws.on("open", () => {
    console.log("[WS] Connected");
    wsReady = true;
    TICKERS.forEach(t => ws.send(JSON.stringify({ type: "subscribe", symbol: t })));
  });
  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw);
      handleTradeMessage(msg);
    } catch (e) {
      console.error("[WS] Failed to parse message", e.message);
    }
  });
  ws.on("error", (err) => console.error("[WS] Error", err.message));
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
// Finnhub REST fallback (staggered, only when WS quiet during regular hours)
// ============================
let finnhubTickerIndex = 0;
function startFinnhubRestPolling() {
  setInterval(() => {
    const wsIsQuiet = (Date.now() - lastWsTradeTime) > WS_QUIET_THRESHOLD_MS;
    if (!wsIsQuiet) return;
    if (!isRegularMarketHours()) return; // Let Twelve Data handle extended hours

    const ticker = TICKERS[finnhubTickerIndex % TICKERS.length];
    finnhubTickerIndex++;
    (async () => {
      try {
        const res = await fetch(`https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${FINNHUB_API_KEY}`);
        const data = await res.json();
        if (data.c) {
          const prevClose = (priceCache[ticker] && priceCache[ticker].prevClose) ? priceCache[ticker].prevClose : (data.pc || data.c);
          const changePct = prevClose ? (((data.c - prevClose) / prevClose) * 100).toFixed(2) : "0.00";
          priceCache[ticker] = { price: data.c, prevClose, changePct };
        }
      } catch (e) {
        console.error(`[POLL] ${ticker} failed`, e.message);
      }
    })();
  }, 1400);
}

// ============================
// Twelve Data polling loop (extended hours only, every 2 minutes per batch)
// Full cycle = ceil(45/8) = 6 batches × 2 min = 12 min per full cycle
// Credits per full cycle = 45, per day max = 750 / 45 = 16 full cycles = fine
// ============================
function startTwelveDataPolling() {
  setInterval(() => {
    const wsIsQuiet = (Date.now() - lastWsTradeTime) > WS_QUIET_THRESHOLD_MS;
    if (!wsIsQuiet) return; // Finnhub WS is active, no need
    if (!isExtendedHours() && !isRegularMarketHours()) return; // Dead zone, skip

    pollTwelveDataBatch();
  }, 2 * 60 * 1000); // Every 2 minutes
}

// ============================
// Candle cache
// Shared across ALL players hitting this proxy. Without this, every popup
// open / timeframe click / 15s auto-refresh from every player was a fresh
// Twelve Data credit, even when 50 players were all looking at the same
// AAPL 1m chart at the same time. Now they all share one cached response
// per ticker+interval until it goes stale.
//
// TTL is matched to how meaningful a fresher candle actually is per
// timeframe — no point spending a credit refreshing 1-day candles every
// 30 seconds, and even 1-minute candles don't need to be fetched faster
// than the client's own 15s auto-refresh cycle.
// ============================
const candleCache = {}; // key: "TICKER:interval" -> { data, fetchedAt }

const CANDLE_TTL_MS = {
  "1min": 20 * 1000,
  "5min": 60 * 1000,
  "15min": 3 * 60 * 1000,
  "30min": 5 * 60 * 1000,
  "1h": 10 * 60 * 1000,
  "1day": 60 * 60 * 1000
};
const DEFAULT_CANDLE_TTL_MS = 60 * 1000;

let twelveDataCandleCreditsUsedToday = 0; // tracked separately for visibility in /health

function getCandleTTL(interval) {
  return CANDLE_TTL_MS[interval] || DEFAULT_CANDLE_TTL_MS;
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
  candleCache[key] = { data, fetchedAt: Date.now() };
}

// ============================
// Routes
// ============================
app.get("/health", (req, res) => res.json({
  status: "ok",
  wsReady,
  wsActive: (Date.now() - lastWsTradeTime) < WS_QUIET_THRESHOLD_MS,
  lastWsTradeMsAgo: Date.now() - lastWsTradeTime,
  cached: Object.keys(priceCache).length,
  twelveDataCreditsUsedToday,
  twelveDataCandleCreditsUsedToday,
  candleCacheEntries: Object.keys(candleCache).length,
  isRegularMarketHours: isRegularMarketHours(),
  isExtendedHours: isExtendedHours()
}));
app.get("/prices", (req, res) => res.json(priceCache));
app.get("/price", (req, res) => {
  const ticker = (req.query.ticker || "").toUpperCase();
  const data = priceCache[ticker];
  res.json(data ? { ticker, ...data } : { error: "No data" });
});
app.get("/candles", async (req, res) => {
  const ticker = (req.query.ticker || "").toUpperCase();
  const interval = req.query.interval || "1min";

  // Map frontend intervals to Twelve Data intervals
  const intervalMap = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "1d": "1day"
  };
  const tdInterval = intervalMap[interval] || interval;

  // Serve from cache if it's still fresh. This is the part that makes N
  // players watching the same ticker/timeframe cost 1 credit instead of N.
  const cached = getCachedCandles(ticker, tdInterval);
  if (cached) {
    return res.json({ ticker, interval: tdInterval, candles: cached, cached: true });
  }

  // Candle counts per timeframe (60 candles each)
  const outputsize = 60;

  try {
    const url = `https://api.twelvedata.com/time_series?symbol=${ticker}&interval=${tdInterval}&outputsize=${outputsize}&apikey=${TWELVE_DATA_API_KEY}`;
    const r = await fetch(url);
    const data = await r.json();

    if (data.status === "error" || !data.values) {
      // Don't cache errors - let the next request try again rather than
      // pinning a failure in place for the full TTL window.
      return res.json({ error: data.message || "No data" });
    }

    // Normalize to simple OHLC array, oldest first
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

    res.json({ ticker, interval: tdInterval, candles });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============================
// Start
// ============================
async function start() {
  await seedPrevClose();
  // If currently in extended hours, do an immediate Twelve Data poll
  if (isExtendedHours()) {
    console.log("[INIT] Extended hours detected, running initial Twelve Data poll...");
    for (let i = 0; i < Math.ceil(TICKERS.length / BATCH_SIZE); i++) {
      await pollTwelveDataBatch();
      await new Promise(r => setTimeout(r, 500)); // Small delay between batches
    }
  }
  connectFinnhub();
  startFinnhubRestPolling();
  startTwelveDataPolling();
  app.listen(PORT, () => console.log(`[SERVER] Ready on ${PORT}`));
}
start();
