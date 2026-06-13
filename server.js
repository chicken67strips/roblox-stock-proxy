const express = require("express");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 8080;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;

const priceCache = {};
const TICKERS = ["AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GME", "AMD", "META"];

let wsReady = false;

setInterval(() => {}, 25000);

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

function connectFinnhub() {
  const ws = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_API_KEY}`);
  ws.on("open", () => {
    console.log("[WS] Connected");
    wsReady = true;
    TICKERS.forEach(t => ws.send(JSON.stringify({ type: "subscribe", symbol: t })));
  });
  ws.on("close", () => setTimeout(connectFinnhub, 5000));
}

// Routes
app.get("/health", (req, res) => res.json({ status: "ok", cached: Object.keys(priceCache) }));
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

async function start() {
  await seedPrevClose();
  connectFinnhub();
  app.listen(PORT, () => console.log(`[SERVER] Ready on ${PORT}`));
}

start();
