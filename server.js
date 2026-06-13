const express = require("express");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 8080;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;

if (!FINNHUB_API_KEY) {
  console.error("[ERROR] FINNHUB_API_KEY not set!");
  process.exit(1);
}

const priceCache = {};
const TICKERS = ["AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GME", "AMD", "META"];

let wsReady = false;

// Railway keep-alive
setInterval(() => {}, 20000);

function connectFinnhub() {
  const ws = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_API_KEY}`);

  ws.on("open", () => {
    console.log("[WS] Connected to Finnhub");
    wsReady = true;
    TICKERS.forEach(t => {
      ws.send(JSON.stringify({ type: "subscribe", symbol: t }));
    });
  });

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === "trade" && msg.data) {
        msg.data.forEach(trade => {
          if (trade.s && trade.p !== undefined) {
            priceCache[trade.s] = { price: trade.p, changePct: "0.00" };
          }
        });
      }
    } catch (e) {}
  });

  ws.on("close", () => {
    wsReady = false;
    setTimeout(connectFinnhub, 5000);
  });
}

// Routes
app.get("/health", (req, res) => res.json({ status: "ok", wsConnected: wsReady }));
app.get("/prices", (req, res) => res.json(priceCache));
app.get("/price", (req, res) => {
  const ticker = (req.query.ticker || "").toUpperCase();
  const data = priceCache[ticker];
  res.json(data ? { ticker, ...data } : { error: "No data" });
});

app.get("/candles", async (req, res) => {
  const ticker = (req.query.ticker || "").toUpperCase();
  if (!ticker) return res.status(400).json({ error: "ticker required" });
  try {
    const r = await fetch(`https://finnhub.io/api/v1/stock/candle?symbol=${ticker}&resolution=5&count=80&token=${FINNHUB_API_KEY}`);
    const data = await r.json();
    res.json(data.s === "ok" ? data : { error: "no data" });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.listen(PORT, () => {
  console.log(`[SERVER] Ready on port ${PORT}`);
  connectFinnhub();
});
