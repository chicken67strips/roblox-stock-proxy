const express = require("express");
const WebSocket = require("ws");
const app = express();
const PORT = process.env.PORT || 8080;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;
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
// Yahoo Finance polling (pre/after hours)
// Fetches all tickers in one request, much more efficient
// ============================
async function pollYahooFinance() {
  try {
    const symbols = TICKERS.join(",");
    const url = `https://query1.finance.yahoo.com/v7/finance/quote?symbols=${symbols}&fields=regularMarketPrice,preMarketPrice,postMarketPrice,marketState,regularMarketPreviousClose`;
    const res = await fetch(url, {
      headers: { "User-Agent": "Mozilla/5.0" }
    });
    const data = await res.json();
    const results = data?.quoteResponse?.result;
    if (!Array.isArray(results)) return;

    for (const quote of results) {
      const ticker = quote.symbol;
      if (!ticker) continue;

      const marketState = quote.marketState; // "PRE", "POST", "REGULAR", "CLOSED"
      const prevClose = quote.regularMarketPreviousClose || (priceCache[ticker] && priceCache[ticker].prevClose) || 0;

      let price = null;
      if (marketState === "PRE" && quote.preMarketPrice) {
        price = quote.preMarketPrice;
      } else if (marketState === "POST" && quote.postMarketPrice) {
        price = quote.postMarketPrice;
      } else if (marketState === "REGULAR" && quote.regularMarketPrice) {
        price = quote.regularMarketPrice;
      } else if (quote.regularMarketPrice) {
        // CLOSED or fallback
        price = quote.regularMarketPrice;
      }

      if (price) {
        const changePct = prevClose ? (((price - prevClose) / prevClose) * 100).toFixed(2) : "0.00";
        priceCache[ticker] = { price, prevClose, changePct, marketState };
      }
    }
    console.log(`[YAHOO] Updated ${results.length} tickers (marketState: ${results[0]?.marketState})`);
  } catch (e) {
    console.error("[YAHOO] Poll failed", e.message);
  }
}

function startPolling() {
  let tickerIndex = 0;

  // Finnhub REST fallback — staggered, only when WS is quiet
  setInterval(() => {
    const wsIsQuiet = (Date.now() - lastWsTradeTime) > WS_QUIET_THRESHOLD_MS;
    if (!wsIsQuiet) return;
    const ticker = TICKERS[tickerIndex % TICKERS.length];
    tickerIndex++;
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

  // Yahoo Finance polling — every 30 seconds, always runs
  // This handles pre-market, after-hours, and fills gaps when Finnhub is stale
  setInterval(pollYahooFinance, 30000);
}

// ============================
// Routes
// ============================
app.get("/health", (req, res) => res.json({
  status: "ok",
  wsReady,
  wsActive: (Date.now() - lastWsTradeTime) < WS_QUIET_THRESHOLD_MS,
  lastWsTradeMsAgo: Date.now() - lastWsTradeTime,
  cached: Object.keys(priceCache).length
}));
app.get("/prices", (req, res) => res.json(priceCache));
app.get("/price", (req, res) => {
  const ticker = (req.query.ticker || "").toUpperCase();
  const data = priceCache[ticker];
  res.json(data ? { ticker, ...data } : { error: "No data" });
});
app.get("/candles", async (req, res) => {
  const ticker = (req.query.ticker || "").toUpperCase();
  try {
    const r = await fetch(`https://finnhub.io/api/v1/stock/candle?symbol=${ticker}&resolution=D&count=30&token=${FINNHUB_API_KEY}`);
    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ============================
// Start
// ============================
async function start() {
  await seedPrevClose();
  // Run Yahoo poll immediately after seed to get after-hours prices right away
  await pollYahooFinance();
  connectFinnhub();
  startPolling();
  app.listen(PORT, () => console.log(`[SERVER] Ready on ${PORT}`));
}
start();
