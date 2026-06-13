const express = require("express");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 3000;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY || "YOUR_API_KEY_HERE";

const priceCache = {};
const TICKERS = ["AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GME", "AMD", "META"];

let ws;
let wsReady = false;

// Keep process alive for Railway
setInterval(() => {}, 30000); // Simple heartbeat

function connectFinnhub() {
  ws = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_API_KEY}`);

  ws.on("open", () => {
    console.log("[WS] Connected to Finnhub");
    wsReady = true;
    TICKERS.forEach(ticker => {
      ws.send(JSON.stringify({ type: "subscribe", symbol: ticker }));
      console.log(`[WS] Subscribed to ${ticker}`);
    });
  });

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === "trade" && msg.data) {
        msg.data.forEach(trade => {
          const symbol = trade.s;
          const price = trade.p;
          if (symbol && price) {
            const prev = priceCache[symbol];
            priceCache[symbol] = {
              price: price,
              prevClose: prev ? prev.prevClose || price : price,
              change: prev ? price - (prev.prevClose || price) : 0,
              changePct: prev && prev.prevClose ? (((price - prev.prevClose) / prev.prevClose) * 100).toFixed(2) : "0.00",
              timestamp: trade.t,
            };
          }
        });
      }
    } catch (e) {
      console.error("[WS] Parse error:", e.message);
    }
  });

  ws.on("error", (err) => console.error("[WS] Error:", err.message));
  ws.on("close", () => {
    console.log("[WS] Disconnected — reconnecting in 5s...");
    wsReady = false;
    setTimeout(connectFinnhub, 5000);
  });
}

async function seedPrevClose() {
  for (const ticker of TICKERS) {
    try {
      const res = await fetch(`https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${FINNHUB_API_KEY}`);
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

// Routes
app.get("/candles", async (req, res) => {
  const ticker = (req.query.ticker || "").toUpperCase();
  const resolution = req.query.resolution || "5";
  const count = parseInt(req.query.count) || 100;

  if (!ticker) return res.status(400).json({ error: "ticker required" });

  try {
    const response = await fetch(`https://finnhub.io/api/v1/stock/candle?symbol=${ticker}&resolution=${resolution}&count=${count}&token=${FINNHUB_API_KEY}`);
    const data = await response.json();
    if (data.s === "ok") {
      res.json({ ticker, c: data.c, h: data.h, l: data.l, o: data.o, t: data.t });
    } else {
      res.status(400).json({ error: "No candle data" });
    }
  } catch (e) {
    console.error("[CANDLES] Error:", e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get("/price", (req, res) => {
  const ticker = (req.query.ticker || "").toUpperCase();
  if (!ticker) return res.status(400).json({ error: "ticker required" });
  const data = priceCache[ticker];
  if (!data) return res.status(404).json({ error: `No data for ${ticker}` });
  res.json({ ticker, ...data });
});

app.get("/prices", (req, res) => res.json(priceCache));

app.get("/health", (req, res) => res.json({ status: "ok", wsConnected: wsReady }));

// Start
async function start() {
  await seedPrevClose();
  connectFinnhub();
  app.listen(PORT, () => {
    console.log(`[SERVER] ✅ Listening on port ${PORT} - Ready for Roblox!`);
  });
}

start().catch(err => console.error("[FATAL]", err));
