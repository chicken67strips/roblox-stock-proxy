const express = require("express");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 8080;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;

if (!FINNHUB_API_KEY || FINNHUB_API_KEY === "YOUR_API_KEY_HERE") {
  console.error("[ERROR] FINNHUB_API_KEY environment variable is missing!");
  process.exit(1);
}

const priceCache = {};
const TICKERS = ["AAPL", "TSLA", "NVDA", "AMZN", "MSFT", "GME", "AMD", "META"];

let ws;
let wsReady = false;

// Railway heartbeat
setInterval(() => {}, 20000);

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
          if (symbol && price != null) {
            const prev = priceCache[symbol];
            priceCache[symbol] = {
              price: price,
              prevClose: prev ? prev.prevClose || price : price,
              changePct: prev && prev.prevClose 
                ? (((price - prev.prevClose) / prev.prevClose) * 100).toFixed(2) 
                : "0.00"
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
    console.log("[WS] Disconnected - reconnecting in 5s");
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
          changePct: data.dp ? data.dp.toFixed(2) : "0.00"
        };
        console.log(`[SEED] ${ticker} prevClose: ${data.pc}`);
      }
    } catch (e) {
      console.error(`[SEED] ${ticker} failed:`, e.message);
    }
  }
}

// === ROUTES ===
app.get("/health", (req, res) => res.json({ status: "ok", wsConnected: wsReady }));

app.get("/prices", (req, res) => res.json(priceCache));

app.get("/price", (req, res) => {
  const ticker = (req.query.ticker || "").toUpperCase();
  const data = priceCache[ticker];
  if (!data) return res.status(404).json({ error: `No data for ${ticker}` });
  res.json({ ticker, ...data });
});

app.get("/candles", async (req, res) => {
  const ticker = (req.query.ticker || "").toUpperCase();
  if (!ticker) return res.status(400).json({ error: "ticker required" });

  try {
    const resp = await fetch(`https://finnhub.io/api/v1/stock/candle?symbol=${ticker}&resolution=5&count=80&token=${FINNHUB_API_KEY}`);
    const data = await resp.json();
    if (data.s === "ok") {
      res.json({ ticker, c: data.c, h: data.h, l: data.l, o: data.o, t: data.t });
    } else {
      res.status(400).json({ error: "No candle data" });
    }
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// === START ===
async function start() {
  await seedPrevClose();
  connectFinnhub();
  app.listen(PORT, () => {
    console.log(`[SERVER] ✅ Ready on port ${PORT}`);
  });
}

start().catch(err => {
  console.error("[FATAL]", err);
  process.exit(1);
});
