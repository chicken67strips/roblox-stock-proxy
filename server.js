const express = require("express");
const WebSocket = require("ws");

const app = express();
const PORT = process.env.PORT || 8080;
const FINNHUB_API_KEY = process.env.FINNHUB_API_KEY;

const priceCache = {};     // Stocks
const cryptoCache = {};    // Crypto

const TICKERS = [
  "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "BRK.B", "AVGO", "LLY", "JPM", "V", "WMT", "UNH", "XOM",
  "AMD", "NFLX", "CRM", "ADBE", "ORCL", "COST", "DIS", "BA", "NKE", "PYPL", "INTC", "UBER", "ABNB", "SBUX", "KO",
  "MASK", "MNTS", "DSY", "INHD", "CLDI", "AZI", "DXST", "WCT", "AIXI", "CODX", "GOVX", "CHAI", "CDLX", "DCX", "CLPR"
];

const CRYPTOS = ["BTC", "ETH", "DOGE", "SOL", "LTC"];

let wsReady = false;
let wsInstance = null;

// ============================
// Seed Stocks
// ============================
async function seedPrevClose() {
  console.log("[SEED] Stocks...");
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
      }
    } catch (e) {}
  }
}

// ============================
// Seed Crypto (freecryptoapi.com)
// ============================
async function seedCrypto() {
  console.log("[SEED] Crypto...");
  try {
    const symbols = CRYPTOS.join(",");
    const res = await fetch(`https://api.freecryptoapi.com/v1/getData?symbol=${symbols}`);
    const data = await res.json();
    
    for (const crypto of CRYPTOS) {
      const info = data[crypto] || {};
      if (info.price) {
        cryptoCache[crypto] = {
          price: parseFloat(info.price),
          changePct: info.change_24h ? info.change_24h.toFixed(2) : "0.00"
        };
        console.log(`[SEED] ${crypto} = $${cryptoCache[crypto].price}`);
      }
    }
  } catch (e) {
    console.error("[SEED Crypto] Failed", e.message);
  }
}

// ============================
// Finnhub WS for Stocks
// ============================
function connectFinnhub() {
  const ws = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_API_KEY}`);
  wsInstance = ws;

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
          if (trade.s && trade.p) {
            const prev = priceCache[trade.s];
            priceCache[trade.s] = {
              price: trade.p,
              prevClose: prev ? prev.prevClose : trade.p,
              changePct: prev && prev.prevClose ? (((trade.p - prev.prevClose) / prev.prevClose) * 100).toFixed(2) : "0.00"
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

// ============================
// Routes
// ============================
app.get("/health", (req, res) => res.json({ 
  status: "ok", 
  wsReady, 
  stocks: Object.keys(priceCache).length,
  cryptos: Object.keys(cryptoCache).length 
}));

app.get("/prices", (req, res) => res.json(priceCache));

app.get("/price", (req, res) => {
  const ticker = (req.query.ticker || "").toUpperCase();
  const data = priceCache[ticker];
  res.json(data ? { ticker, ...data } : { error: "No data" });
});

// Crypto Routes
app.get("/crypto", (req, res) => res.json(cryptoCache));

app.get("/crypto/price", (req, res) => {
  const symbol = (req.query.symbol || "").toUpperCase();
  const data = cryptoCache[symbol];
  res.json(data ? { symbol, ...data } : { error: "No data" });
});

// Candles (stocks)
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

// Start
async function start() {
  await seedPrevClose();
  await seedCrypto();
  connectFinnhub();
  
  app.listen(PORT, () => console.log(`[SERVER] Ready on ${PORT}`));
}

start();
