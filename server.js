const express = require("express");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 8080;

const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
const TWELVE_DATA_API_KEY = process.env.TWELVE_DATA_API_KEY;
const FREECRYPTO_API_KEY = process.env.FREECRYPTO_API_KEY;
const FREECRYPTO_BASE_URL = "https://api.freecryptoapi.com/v1";

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
// Market hours check UTC
// ============================
function getUTCComponents() {
  const now = new Date();
  return {
    wday: now.getUTCDay(),
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

  return (
    (totalMin >= (8 * 60) && totalMin < (14 * 60 + 30)) ||
    (totalMin >= (20 * 60) && totalMin < (23 * 60))
  );
}

// ============================
// Seed from Finnhub
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

const MAX_CREDITS_PER_DAY = 750;

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

  twelveDataBatchIndex =
    (twelveDataBatchIndex + 1) % Math.ceil(TICKERS.length / BATCH_SIZE);

  if (batch.length === 0) return;

  try {
    const symbols = batch.join(",");
    const url = `https://api.twelvedata.com/quote?symbol=${symbols}&apikey=${TWELVE_DATA_API_KEY}`;
    const data = await tdRequest(url, TD_PRIORITY.quote);

    const results = batch.length === 1 ? { [batch[0]]: data } : data;

    let updated = 0;

    for (const ticker of batch) {
      const quote = results[ticker];
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
        priceCache[ticker] = {
          price,
          prevClose,
          changePct
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
// Finnhub WebSocket
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

    const changePct = prevClose
      ? (((price - prevClose) / prevClose) * 100).toFixed(2)
      : "0.00";

    priceCache[ticker] = {
      price,
      prevClose,
      changePct
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
      ws.send(JSON.stringify({
        type: "subscribe",
        symbol: ticker
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

    const ticker = TICKERS[finnhubTickerIndex % TICKERS.length];
    finnhubTickerIndex++;

    (async () => {
      try {
        const res = await fetch(`https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${FINNHUB_API_KEY}`);
        const data = await res.json();

        if (data.c) {
          const prevClose =
            priceCache[ticker] && priceCache[ticker].prevClose
              ? priceCache[ticker].prevClose
              : data.pc || data.c;

          const changePct = prevClose
            ? (((data.c - prevClose) / prevClose) * 100).toFixed(2)
            : "0.00";

          priceCache[ticker] = {
            price: data.c,
            prevClose,
            changePct
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
  "1min": 20 * 1000,
  "5min": 60 * 1000,
  "15min": 3 * 60 * 1000,
  "30min": 5 * 60 * 1000,
  "1h": 10 * 60 * 1000,
  "1day": 60 * 60 * 1000
};

const DEFAULT_CANDLE_TTL_MS = 60 * 1000;

let twelveDataCandleCreditsUsedToday = 0;

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

  candleCache[key] = {
    data,
    fetchedAt: Date.now()
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
    twelveDataQueueDepth: tdQueueDepth(),
    twelveDataCallsInLastMinute: tdCallsInLastMinute(),
    candleCacheEntries: Object.keys(candleCache).length,
    cryptoCached: Object.keys(cryptoPriceCache).length,
    cryptoSourceSample: cryptoPriceCache.BTC && cryptoPriceCache.BTC.source,
    cryptoCandleCacheEntries: Object.keys(cryptoCandleCache).length,
    isRegularMarketHours: isRegularMarketHours(),
    isExtendedHours: isExtendedHours()
  });
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

app.get("/prices", (req, res) => {
  res.json(priceCache);
});

app.get("/price", (req, res) => {
  const ticker = String(req.query.ticker || "").toUpperCase();
  const data = priceCache[ticker];

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

  const cached = getCachedCandles(ticker, tdInterval);

  if (cached) {
    return res.json({
      ticker,
      interval: tdInterval,
      candles: cached,
      cached: true
    });
  }

  const outputsize = 200;

  const url =
    `https://api.twelvedata.com/time_series` +
    `?symbol=${ticker}` +
    `&interval=${tdInterval}` +
    `&outputsize=${outputsize}` +
    `&apikey=${TWELVE_DATA_API_KEY}`;

  try {
    const data = await tdRequest(url, TD_PRIORITY.candle);

    if (data.status === "error" || !data.values) {
      return res.json({
        error: data.message || "No data"
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
      candles
    });
  } catch (e) {
    res.json({
      error: e.message
    });
  }
});

// ============================
// Start
// ============================
async function start() {
  await seedPrevClose();

  if (isExtendedHours()) {
    console.log("[INIT] Extended hours detected, running initial Twelve Data poll...");

    for (let i = 0; i < Math.ceil(TICKERS.length / BATCH_SIZE); i++) {
      await pollTwelveDataBatch();
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }

  connectFinnhub();
  startFinnhubRestPolling();
  startTwelveDataPolling();

  app.listen(PORT, () => {
    console.log(`[SERVER] Ready on ${PORT}`);
  });
}

start();
