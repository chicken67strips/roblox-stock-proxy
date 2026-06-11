const express = require("express");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 3000;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY || "YOUR_API_KEY_HERE";

// ── In-memory price cache ──────────────────────────────────────────────────
// { "AAPL": { price: 213.45, prevClose: 210.00, timestamp: 1718000000000 } }
const priceCache = {};

// Tickers to subscribe to — edit this list freely
const TICKERS = ["AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GME", "AMD", "META"];

// ── Finnhub WebSocket connection ───────────────────────────────────────────
let ws;
let wsReady = false;

function connectFinnhub() {
  ws = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_API_KEY}`);

  ws.on("open", () => {
    console.log("[WS] Connected to Finnhub");
    wsReady = true;
    // Subscribe to each ticker
    TICKERS.forEach((ticker) => {
      ws.send(JSON.stringify({ type: "subscribe", symbol: ticker }));
      console.log(`[WS] Subscribed to ${ticker}`);
    });
  });

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === "trade" && msg.data) {
        msg.data.forEach((trade) => {
          const symbol = trade.s;
          const price = trade.p;
          const timestamp = trade.t;
          if (symbol && price) {
            const prev = priceCache[symbol];
            priceCache[symbol] = {
              price: price,
              prevClose: prev ? prev.prevClose || price : price,
              change: prev ? price - (prev.prevClose || price) : 0,
              changePct: prev && prev.prevClose
                ? (((price - prev.prevClose) / prev.prevClose) * 100).toFixed(2)
                : "0.00",
              timestamp: timestamp,
            };
          }
        });
      }
    } catch (e) {
      console.error("[WS] Parse error:", e.message);
    }
  });

  ws.on("error", (err) => {
    console.error("[WS] Error:", err.message);
    wsReady = false;
  });

  ws.on("close", () => {
    console.log("[WS] Disconnected — reconnecting in 5s...");
    wsReady = false;
    setTimeout(connectFinnhub, 5000);
  });
}

// Seed prevClose via REST on startup so change% is accurate from the start
async function seedPrevClose() {
  for (const ticker of TICKERS) {
    try {
      const res = await fetch(
        `https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${FINNHUB_API_KEY}`
      );
      const data = await res.json();
      if (data.pc) {
        priceCache[ticker] = {
          price: data.c || data.pc,
          prevClose: data.pc,
          change: data.d || 0,
          changePct: data.dp ? data.dp.toFixed(2) : "0.00",
          timestamp: Date.now(),
        };
        console.log(`[SEED] ${ticker} prevClose: ${data.pc}`);
      }
    } catch (e) {
      console.error(`[SEED] Failed for ${ticker}:`, e.message);
    }
  }
}

// ── Routes ─────────────────────────────────────────────────────────────────

// Single ticker:  GET /price?ticker=AAPL
app.get("/price", (req, res) => {
  const ticker = (req.query.ticker || "").toUpperCase();
  if (!ticker) return res.status(400).json({ error: "ticker param required" });

  const data = priceCache[ticker];
  if (!data) return res.status(404).json({ error: `No data for ${ticker}` });

  res.json({ ticker, ...data });
});

// All tickers:    GET /prices
app.get("/prices", (req, res) => {
  res.json(priceCache);
});

// Health check:   GET /health
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    wsConnected: wsReady,
    cachedTickers: Object.keys(priceCache),
  });
});

// ── Start ──────────────────────────────────────────────────────────────────
seedPrevClose().then(() => {
  connectFinnhub();
  app.listen(PORT, () => {
    console.log(`[SERVER] Listening on port ${PORT}`);
  });
});
