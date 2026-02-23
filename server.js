// server.js - BtcTurk & OKX Arbitraj + 7/24 İSTATİSTİK KAYDI
const express = require('express');
const cors = require('cors');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

let wsConnected = false;

// ===== HEALTH CHECK =====
app.get('/health', (req, res) => res.json({ status: 'OK', websocket: { connected: wsConnected } }));
app.get('/healthz', (req, res) => res.status(200).send('OK'));

// ===== VERİ DEPOLAMA =====
let allSignals = [];
let activeSignals = new Map();
let stats = {
  totalSignals: 0,
  totalValue: 0,
  bySymbol: {},
  hourly: {},
  daily: {},
  maxPct: 0,
  maxPctSymbol: '',
  maxPctTime: null,
  startTime: Date.now()
};

// ===== STREAM & CACHE =====
const streamCache = new Map();
const subscribedChannels = new Set();
const depthCache = new Map();
let ws = null;

let wsMessageCount = 0;
let wsLastMessageTime = 0;
let arbitrageCheckCount = 0;

let CONFIG = {
  symbols: ['ACM', 'ASR', 'ATM', 'BAR', 'CITY', 'JUV', 'LUNA', 'PSG'],
  minPct: 0.4,
  checkInterval: 500
};

// OKX fiyat cache - server tarafında tutulur, frontend proxy'den alır
let okxPrices = {};
let okxLastFetch = 0;
const OKX_CACHE_TIME = 500;

let usdtTry = 0;
let lastCheck = 0;

// ===== HELPER =====
function convertToBtcTurkSymbol(sym) {
  return sym.toUpperCase().replace('_TL', 'TRY').replace('_', '');
}

function calculateDepth(market, bids, asks) {
  if (!bids || !asks) return;
  const topBidQty = bids.length > 0 ? parseFloat(bids[0][1]) : 0;
  const topAskQty = asks.length > 0 ? parseFloat(asks[0][1]) : 0;
  const topBidPrice = bids.length > 0 ? parseFloat(bids[0][0]) : 0;
  const topAskPrice = asks.length > 0 ? parseFloat(asks[0][0]) : 0;
  depthCache.set(market, {
    topBidQty, topAskQty,
    topBidValue: topBidPrice * topBidQty,
    topAskValue: topAskPrice * topAskQty,
    topBidPrice, topAskPrice,
    timestamp: Date.now()
  });
}

// ===== BTCTURK WEBSOCKET =====
let wsReconnectTimer = null;
let wsReconnectAttempts = 0;
let wsPingInterval = null;

function connectWebSocket() {
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
  console.log('🔌 BtcTurk WebSocket bağlanıyor... (Deneme:', wsReconnectAttempts + 1, ')');
  try {
    if (ws) { try { ws.terminate(); } catch(e) {} ws = null; }
    if (wsPingInterval) { clearInterval(wsPingInterval); wsPingInterval = null; }

    ws = new WebSocket('wss://ws-feed-pro.btcturk.com');

    const connectTimeout = setTimeout(() => {
      if (ws && ws.readyState !== WebSocket.OPEN) {
        console.log('⏱️ WebSocket timeout');
        try { ws.terminate(); } catch(e) {}
        ws = null; wsConnected = false;
        scheduleReconnect();
      }
    }, 5000);

    ws.on('open', () => {
      clearTimeout(connectTimeout);
      wsConnected = true;
      wsReconnectAttempts = 0;
      wsMessageCount = 0;
      console.log('✅ BtcTurk WebSocket bağlandı!');
      subscribeToMarket('USDTTRY');
      CONFIG.symbols.forEach(sym => subscribeToMarket(`${sym}TRY`));
      wsPingInterval = setInterval(() => {
        if (ws && ws.readyState === WebSocket.OPEN) try { ws.ping(); } catch(e) {}
      }, 15000);
    });

    ws.on('message', (data) => {
      try {
        const now = Date.now();
        wsMessageCount++;
        wsLastMessageTime = now;
        const msg = JSON.parse(data.toString());
        if (!Array.isArray(msg) || msg.length < 2) return;
        const [type, payload] = msg;
        let dataUpdated = false;

        if ((type === 422 || type === 431) && payload && payload.PS) {
          const market = payload.PS;
          const bids = (payload.BO || []).map(o => Array.isArray(o) ? [o[0], o[1]] : [o.P, o.A])
            .sort((a, b) => parseFloat(b[0]) - parseFloat(a[0])).slice(0, 20);
          const asks = (payload.AO || []).map(o => Array.isArray(o) ? [o[0], o[1]] : [o.P, o.A])
            .sort((a, b) => parseFloat(a[0]) - parseFloat(b[0])).slice(0, 20);
          streamCache.set(market, { bids, asks, timestamp: now });
          calculateDepth(market, bids, asks);
          dataUpdated = true;
        }

        if (type === 432 && payload && payload.PS) {
          const market = payload.PS;
          const existing = streamCache.get(market) || { bids: [], asks: [], timestamp: 0 };
          if (payload.BO && payload.BO.length > 0) {
            const bidMap = new Map(existing.bids.map(b => [b[0], b[1]]));
            payload.BO.forEach(o => {
              const [price, amount] = Array.isArray(o) ? [o[0], o[1]] : [o.P, o.A];
              if (parseFloat(amount) === 0) bidMap.delete(price);
              else bidMap.set(price, amount);
            });
            existing.bids = Array.from(bidMap.entries()).sort((a, b) => parseFloat(b[0]) - parseFloat(a[0])).slice(0, 20);
          }
          if (payload.AO && payload.AO.length > 0) {
            const askMap = new Map(existing.asks.map(a => [a[0], a[1]]));
            payload.AO.forEach(o => {
              const [price, amount] = Array.isArray(o) ? [o[0], o[1]] : [o.P, o.A];
              if (parseFloat(amount) === 0) askMap.delete(price);
              else askMap.set(price, amount);
            });
            existing.asks = Array.from(askMap.entries()).sort((a, b) => parseFloat(a[0]) - parseFloat(b[0])).slice(0, 20);
          }
          existing.timestamp = now;
          streamCache.set(market, existing);
          calculateDepth(market, existing.bids, existing.asks);
          dataUpdated = true;
        }

        if (dataUpdated) checkArbitrageInstant();
      } catch (e) {}
    });

    ws.on('close', (code) => {
      wsConnected = false;
      if (wsPingInterval) { clearInterval(wsPingInterval); wsPingInterval = null; }
      console.log('🔴 WebSocket kapandı. Kod:', code);
      scheduleReconnect();
    });

    ws.on('error', (err) => { console.error('❌ WS Error:', err.message); wsConnected = false; });
    ws.on('pong', () => { wsLastMessageTime = Date.now(); });

  } catch (err) {
    console.error('❌ WS Create Error:', err.message);
    wsConnected = false;
    scheduleReconnect();
  }
}

function scheduleReconnect() {
  if (wsReconnectTimer) return;
  wsReconnectAttempts++;
  const delay = Math.min(1000 * Math.pow(1.5, wsReconnectAttempts - 1), 10000);
  console.log(`🔄 ${(delay/1000).toFixed(1)}sn sonra yeniden bağlanılacak...`);
  wsReconnectTimer = setTimeout(() => { wsReconnectTimer = null; connectWebSocket(); }, delay);
}

setInterval(() => {
  if (!wsConnected || !ws || ws.readyState !== WebSocket.OPEN) {
    if (ws) { try { ws.terminate(); } catch(e) {} ws = null; }
    if (wsPingInterval) { clearInterval(wsPingInterval); wsPingInterval = null; }
    wsConnected = false;
    connectWebSocket();
  }
}, 15000);

function subscribeToMarket(market) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  if (subscribedChannels.has(market)) return true;
  try {
    ws.send(JSON.stringify([151, { type: 151, channel: "orderbook", event: market, join: true }]));
    subscribedChannels.add(market);
    console.log(`📡 ${market} abone olundu`);
    return true;
  } catch (e) { return false; }
}

// ===== OKX FİYATLARI (SERVER CACHE) =====
async function fetchOkxPrices() {
  if (Date.now() - okxLastFetch < OKX_CACHE_TIME) return;
  try {
    const requests = CONFIG.symbols.map(s =>
      fetch(`https://www.okx.com/api/v5/market/ticker?instId=${s}-USDT`)
        .then(r => r.json())
        .then(data => {
          if (data.data && data.data.length > 0) {
            const item = data.data[0];
            okxPrices[s] = {
              bid: parseFloat(item.bidPx),
              ask: parseFloat(item.askPx),
              ts: Date.now()
            };
          }
        })
        .catch(() => {})
    );
    await Promise.all(requests);
    okxLastFetch = Date.now();
  } catch (e) {
    console.error('OKX hatası:', e.message);
  }
}

// ===== USDT/TRY =====
function getUsdtTry() {
  const cached = streamCache.get('USDTTRY');
  if (cached && cached.bids && cached.bids.length > 0) {
    const bid = parseFloat(cached.bids[0][0]);
    const ask = parseFloat(cached.asks[0][0]);
    usdtTry = (bid + ask) / 2;
  }
  return usdtTry;
}

// ===== ARBİTRAJ KONTROLÜ =====
function checkArbitrageInstant() {
  try {
    if (!wsConnected) return;
    const usdt = getUsdtTry();
    if (usdt <= 0) return;

    arbitrageCheckCount++;
    const now = Date.now();
    const hour = new Date().toISOString().slice(0, 13);
    const day = new Date().toISOString().slice(0, 10);
    const currentActive = new Set();

    CONFIG.symbols.forEach(sym => {
      const btcturkMarket = `${sym}TRY`;
      const cached = streamCache.get(btcturkMarket);
      const okx = okxPrices[sym];

      if (!cached || !cached.bids || cached.bids.length === 0) return;
      if (!okx) return;

      const pBid = parseFloat(cached.bids[0][0]);
      const pAsk = parseFloat(cached.asks[0][0]);
      const pBidQty = parseFloat(cached.bids[0][1]);
      const pAskQty = parseFloat(cached.asks[0][1]);

      const bBidTL = okx.bid * usdt;
      const bAskTL = okx.ask * usdt;

      const diff1 = pBid - bAskTL;
      const diff2 = bBidTL - pAsk;

      let pct = 0, direction = '', qty = 0, value = 0;

      if (diff1 > diff2 && diff1 > 0) {
        pct = (diff1 / bAskTL) * 100;
        direction = 'sell'; qty = pBidQty; value = pBid * pBidQty;
      } else if (diff2 > 0) {
        pct = (diff2 / pAsk) * 100;
        direction = 'buy'; qty = pAskQty; value = pAsk * pAskQty;
      }

      if (pct >= CONFIG.minPct) {
        currentActive.add(sym);
        if (!activeSignals.has(sym)) {
          activeSignals.set(sym, { sym, direction, startTime: now, startPct: pct, maxPct: pct, qty, value });
          console.log(`📈 [${sym}] ${direction === 'sell' ? 'SAT' : 'AL'} %${pct.toFixed(2)} | ${qty.toFixed(2)} adet | ${value.toFixed(0)} TL`);
        } else {
          const signal = activeSignals.get(sym);
          if (pct > signal.maxPct) { signal.maxPct = pct; signal.qty = qty; signal.value = value; }
          signal.currentPct = pct;
          signal.direction = direction;
        }
      }
    });

    for (const [sym, signal] of activeSignals) {
      if (!currentActive.has(sym)) {
        const duration = (now - signal.startTime) / 1000;
        const record = {
          sym, direction: signal.direction, pct: signal.maxPct,
          qty: signal.qty, value: signal.value, duration,
          timestamp: signal.startTime, endTime: now, hour, day
        };
        allSignals.push(record);
        if (allSignals.length > 50000) allSignals = allSignals.slice(-40000);

        stats.totalSignals++;
        stats.totalValue += signal.value;
        if (!stats.bySymbol[sym]) stats.bySymbol[sym] = { count: 0, totalDuration: 0, maxPct: 0, totalValue: 0, avgPct: 0 };
        const s = stats.bySymbol[sym];
        s.count++; s.totalDuration += duration; s.totalValue += signal.value;
        if (signal.maxPct > s.maxPct) s.maxPct = signal.maxPct;
        s.avgPct = ((s.avgPct * (s.count - 1)) + signal.maxPct) / s.count;

        if (!stats.hourly[hour]) stats.hourly[hour] = { count: 0, value: 0 };
        stats.hourly[hour].count++; stats.hourly[hour].value += signal.value;

        if (!stats.daily[day]) stats.daily[day] = { count: 0, value: 0, maxPct: 0 };
        stats.daily[day].count++; stats.daily[day].value += signal.value;
        if (signal.maxPct > stats.daily[day].maxPct) stats.daily[day].maxPct = signal.maxPct;

        if (signal.maxPct > stats.maxPct) {
          stats.maxPct = signal.maxPct; stats.maxPctSymbol = sym; stats.maxPctTime = signal.startTime;
        }
        console.log(`✅ [${sym}] Kapandı: %${signal.maxPct.toFixed(2)} | ${duration.toFixed(1)}sn | Toplam: ${stats.totalSignals}`);
        activeSignals.delete(sym);
      }
    }
    lastCheck = now;
  } catch (e) {
    console.error('Arbitraj kontrol hatası:', e.message);
  }
}

async function checkArbitrage() {
  await fetchOkxPrices();
  checkArbitrageInstant();
}

// ===== HTML SERVE =====
let indexHtml = '<h1>Loading...</h1>';
let statsHtml = '<h1>Loading...</h1>';
try {
  const indexPath = path.join(__dirname, 'index.html');
  if (fs.existsSync(indexPath)) { indexHtml = fs.readFileSync(indexPath, 'utf8'); console.log('✅ index.html yüklendi'); }
  const statsPath = path.join(__dirname, 'stats.html');
  if (fs.existsSync(statsPath)) { statsHtml = fs.readFileSync(statsPath, 'utf8'); console.log('✅ stats.html yüklendi'); }
} catch (e) {}

app.get('/', (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(indexHtml); });
app.get('/stats', (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(statsHtml); });

// ===== API ENDPOINTS =====
app.get('/api/btcturk/orderbook', (req, res) => {
  const { market } = req.query;
  if (!market) return res.status(400).json({ error: 'market gerekli' });
  const m = market.includes('_') ? convertToBtcTurkSymbol(market) : market.toUpperCase();
  subscribeToMarket(m);
  const cached = streamCache.get(m);
  res.json(cached ? { bids: cached.bids, asks: cached.asks } : { bids: [], asks: [] });
});

app.post('/api/btcturk/batch', (req, res) => {
  const { markets } = req.body;
  if (!Array.isArray(markets)) return res.status(400).json({ error: 'markets array gerekli' });
  markets.forEach(m => subscribeToMarket(m.includes('_') ? convertToBtcTurkSymbol(m) : m.toUpperCase()));
  const results = markets.map(market => {
    const m = market.includes('_') ? convertToBtcTurkSymbol(market) : market.toUpperCase();
    const cached = streamCache.get(m);
    const depth = depthCache.get(m);
    if (cached && cached.bids && cached.bids.length > 0) {
      return { market, success: true, data: { bids: cached.bids, asks: cached.asks }, depth, cached: true };
    }
    return { market, success: false, error: 'Veri yok' };
  });
  res.json({ success: true, results });
});

// ===== OKX PROXY (CORS çözümü - server cache'den döner) =====
app.post('/api/okx/batch', (req, res) => {
  const { symbols } = req.body;
  if (!Array.isArray(symbols)) return res.status(400).json({ error: 'symbols array gerekli' });
  const results = symbols.map(sym => {
    const p = okxPrices[sym];
    if (p && p.bid > 0) return { sym, success: true, bidPx: p.bid, askPx: p.ask, ts: p.ts };
    return { sym, success: false };
  });
  res.json({ success: true, results, serverTs: Date.now() });
});

// ===== İSTATİSTİK API =====
app.get('/api/stats', (req, res) => {
  const uptime = Math.floor((Date.now() - stats.startTime) / 1000);
  const checksPerSec = uptime > 0 ? (arbitrageCheckCount / uptime).toFixed(1) : 0;
  res.json({
    ...stats, uptime,
    uptimeStr: `${Math.floor(uptime/3600)}s ${Math.floor((uptime%3600)/60)}d ${uptime%60}sn`,
    activeCount: activeSignals.size,
    activeSignals: Array.from(activeSignals.values()),
    wsConnected, usdtTry, lastCheck,
    wsMessageCount, wsLastMessageTime, arbitrageCheckCount, checksPerSec,
    cacheSize: streamCache.size
  });
});

app.get('/api/stats/signals', (req, res) => {
  const { limit = 100, sym, day } = req.query;
  let filtered = allSignals;
  if (sym) filtered = filtered.filter(s => s.sym === sym.toUpperCase());
  if (day) filtered = filtered.filter(s => s.day === day);
  res.json({ total: filtered.length, signals: filtered.slice(-parseInt(limit)).reverse() });
});

app.get('/api/stats/daily', (req, res) => res.json(stats.daily));

app.get('/api/stats/hourly', (req, res) => {
  const hours = Object.entries(stats.hourly).sort((a, b) => b[0].localeCompare(a[0])).slice(0, 24);
  res.json(Object.fromEntries(hours));
});

app.get('/api/stats/symbols', (req, res) => res.json(stats.bySymbol));

app.get('/api/stats/export', (req, res) => {
  res.setHeader('Content-Type', 'application/json');
  res.setHeader('Content-Disposition', `attachment; filename=arbitraj-stats-${new Date().toISOString().slice(0,10)}.json`);
  res.json({ exportTime: new Date().toISOString(), stats, signals: allSignals });
});

// ===== CONFIG API =====
app.get('/api/config', (req, res) => res.json(CONFIG));

app.post('/api/config', (req, res) => {
  const { symbols, minPct, checkInterval } = req.body;
  if (symbols) { CONFIG.symbols = symbols; symbols.forEach(sym => subscribeToMarket(`${sym}TRY`)); }
  if (minPct !== undefined) CONFIG.minPct = parseFloat(minPct);
  if (checkInterval) CONFIG.checkInterval = parseInt(checkInterval);
  console.log('⚙️ Config güncellendi:', CONFIG);
  res.json({ success: true, config: CONFIG });
});

app.use((req, res) => res.status(404).json({ error: 'Not found' }));

// ===== START =====
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log('✅ Server dinliyor - Port:', PORT);
  connectWebSocket();
  setInterval(checkArbitrage, CONFIG.checkInterval);
  console.log('📊 7/24 istatistik kaydı aktif!');
  console.log(`📋 Semboller: ${CONFIG.symbols.join(', ')}`);
  console.log(`📉 Min %: ${CONFIG.minPct}`);
});

server.on('error', (err) => console.error('❌ Server Error:', err.message));
process.on('uncaughtException', (err) => console.error('⚠️ Exception:', err.message));
process.on('unhandledRejection', (reason) => console.error('⚠️ Rejection:', reason));

setInterval(() => {
  const now = new Date().toLocaleTimeString('tr-TR');
  const mem = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(1);
  const uptime = (Date.now() - stats.startTime) / 1000;
  const checksPerSec = uptime > 0 ? (arbitrageCheckCount / uptime).toFixed(1) : 0;
  console.log(`[${now}] Sinyal: ${stats.totalSignals} | Aktif: ${activeSignals.size} | WS: ${wsConnected ? '✅' : '❌'} | Check/sn: ${checksPerSec} | RAM: ${mem}MB`);
}, 30000);
