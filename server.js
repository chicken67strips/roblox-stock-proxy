const express = require("express");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 8080;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;

const priceCache = {};
const TICKERS = ["AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GME", "AMD", "META"];

let wsReady = false;

setInterval(() => {}, 20000);

function connectFinnhub() {
  const ws = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_API_KEY}`);

  ws.on("open", () => {
    console.log("[WS] Connected to Finnhub");
    wsReady = true;
    TICKERS.forEach(t => ws.send(JSON.stringify({ type: "subscribe", symbol: t })));
  });

  ws.on("message", (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === "trade" && msg.data) {
        msg.data.forEach(trade => {
          if (trade.s && trade.p !== undefined) {
            const prev = priceCache[trade.s];
            priceCache[trade.s] = {
              price: trade.p,
              prevClose: prev ? prev.prevClose : trade.p,
              changePct: prev ? (((trade.p - prev.prevClose) / prev.prevClose) * 100).toFixed(2) : "0.00"
            };
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

// Seed with real data
async function seedData() {
  for (const ticker of TICKERS) {
    try {
      const res = await fetch(`https://finnhub.io/api/v1/quote?symbol=${ticker}&token=${FINNHUB_API_KEY}`);
      const data = await res.json();
      if (data.c) {
        priceCache[ticker] = {
          price: data.c,
          prevClose: data.pc || data.c,
          changePct: data.dp ? data.dp.toFixed(2) : "0.00"
        };
        console.log(`[SEED] ${ticker} ${data.c}`);
      }
    } catch (e) {}
  }
}

// Routes
app.get("/health", (req, res) => res.json({ status: "ok", wsConnected: wsReady, tickers: Object.keys(priceCache) }));

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
    const r = await fetch(`https://finnhub.io/api/v1/stock/candle?symbol=${ticker}&resolution=D&count=30&token=${FINNHUB_API_KEY}`);
    const data = await r.json();
    res.json(data);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

async function start() {
  await seedData();
  connectFinnhub();
  app.listen(PORT, () => console.log(`[SERVER] Ready on ${PORT}`));
}

start();
