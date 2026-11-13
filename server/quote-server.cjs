// Local market-data service:
//  - static hosting for the receiving app (web/)
//  - WebSocket push with per-symbol independent sequence numbers
//  - HTTP backfill by sequence range
//  - fault injection: disconnect, network blackhole, duplicate, reorder,
//    conflicting payload, history deletion
// Zero runtime dependencies (Node >= 21 for global WebSocket on the client side).

const http = require('node:http');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { URL } = require('node:url');

const { acceptKey, parseFrames, buildFrame, buildCloseFrame, FrameError } = require('./ws.cjs');

const PORT = Number(process.env.PORT || 8080);
const TICK_MS = Number(process.env.TICK_MS || 1000);
const RETENTION = Number(process.env.RETENTION || 60); // messages kept per symbol
const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'server-state.json');
const WEB_DIR = path.join(__dirname, '..', 'web');

const SYMBOLS = [
  { symbol: 'BTC-USD', base: 67000 },
  { symbol: 'ETH-USD', base: 3500 },
  { symbol: 'SOL-USD', base: 165 },
  { symbol: 'BNB-USD', base: 600 },
];

// ---------------------------------------------------------------------------
// Per-symbol state
// ---------------------------------------------------------------------------

/**
 * state[symbol] = {
 *   seq: number (latest produced),
 *   history: Map<seq, record>,
 *   walk: current random-walk price
 * }
 */
const state = new Map();

function loadState() {
  try {
    const raw = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    for (const [symbol, saved] of Object.entries(raw.symbols || {})) {
      const history = new Map();
      for (const rec of saved.history || []) history.set(rec.seq, rec);
      state.set(symbol, { seq: saved.seq, history, walk: saved.walk });
    }
  } catch {
    // first run: fall through to defaults
  }
  for (const { symbol, base } of SYMBOLS) {
    if (!state.has(symbol)) state.set(symbol, { seq: 0, history: new Map(), walk: base });
  }
}

let saveTimer = null;
function persistSoon() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    const out = { symbols: {} };
    for (const [symbol, s] of state.entries()) {
      out.symbols[symbol] = {
        seq: s.seq,
        walk: s.walk,
        history: [...s.history.values()],
      };
    }
    fs.mkdirSync(DATA_DIR, { recursive: true });
    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(out));
    fs.renameSync(tmp, DATA_FILE); // atomic replace
  }, 800);
}

function earliestSeq(s) {
  return s.history.size ? Math.min(...s.history.keys()) : s.seq + 1;
}

function produceTick(symbol) {
  const s = state.get(symbol);
  // bounded random walk, ~0.12% volatility per tick
  const drift = (Math.random() - 0.5) * 0.0024;
  s.walk = Math.max(0.01, s.walk * (1 + drift));
  s.seq += 1;
  const price = Number(s.walk.toFixed(2));
  const spread = Number((price * 0.0004).toFixed(2));
  const record = {
    type: 'quote',
    symbol,
    seq: s.seq,
    ts: Date.now(),
    price,
    bid: Number((price - spread).toFixed(2)),
    ask: Number((price + spread).toFixed(2)),
    volume: Number((Math.random() * 12 + 0.5).toFixed(3)),
  };
  s.history.set(s.seq, record);
  while (s.history.size > RETENTION) {
    s.history.delete(Math.min(...s.history.keys()));
  }
  persistSoon();
  return record;
}

// ---------------------------------------------------------------------------
// WebSocket clients
// ---------------------------------------------------------------------------

class Client {
  constructor(socket) {
    this.socket = socket;
    this.alive = true;
    this.buffer = Buffer.alloc(0);
    this.fragments = [];
    this.fragmentOpcode = null;
    this.helloed = false;
  }

  send(obj) {
    if (this.socket.destroyed || globalFlags.blackhole) return;
    try {
      this.socket.write(buildFrame(JSON.stringify(obj), { opcode: 0x1 }));
    } catch {
      this.destroy();
    }
  }

  destroy() {
    try {
      this.socket.destroy();
    } catch {
      /* noop */
    }
  }
}

const clients = new Set();
const globalFlags = { blackhole: false };

function broadcast(obj) {
  const frame = buildFrame(JSON.stringify(obj), { opcode: 0x1 });
  for (const c of clients) {
    if (c.socket.destroyed || globalFlags.blackhole) continue;
    try {
      c.socket.write(frame);
    } catch {
      c.destroy();
    }
  }
}

function handleFrame(client, frame) {
  // control frames
  if (frame.opcode === 0x8) {
    let code = 1000;
    if (frame.payload.length >= 2) code = frame.payload.readUInt16BE(0);
    try {
      client.socket.write(buildCloseFrame(code, 'bye'));
    } catch {
      /* noop */
    }
    client.destroy();
    return;
  }
  if (frame.opcode === 0x9) {
    if (!client.socket.destroyed) {
      client.socket.write(buildFrame(frame.payload, { opcode: 0xa }));
    }
    return;
  }
  if (frame.opcode === 0xa) return; // pong

  // data frames, with fragmentation support
  if (frame.opcode === 0x0) {
    if (client.fragmentOpcode === null) throw new FrameError('continuation without start');
    client.fragments.push(frame.payload);
    if (!frame.fin) return;
    const payload = Buffer.concat(client.fragments).toString('utf8');
    client.fragmentOpcode = null;
    client.fragments = [];
    handleMessage(client, payload);
    return;
  }
  if (!frame.fin) {
    client.fragmentOpcode = frame.opcode;
    client.fragments = [frame.payload];
    return;
  }
  if (frame.opcode !== 0x1) throw new FrameError('binary frames unsupported', 1003);
  handleMessage(client, frame.payload.toString('utf8'));
}

function handleMessage(client, text) {
  let msg;
  try {
    msg = JSON.parse(text);
  } catch {
    throw new FrameError('invalid json', 1007);
  }
  if (msg.type === 'hello') {
    client.helloed = true;
    client.cursors = msg.cursors || {};
    const positions = {};
    for (const [symbol, s] of state.entries()) {
      positions[symbol] = { latestSeq: s.seq, earliestSeq: earliestSeq(s) };
    }
    client.send({ type: 'hello_ack', positions, ts: Date.now() });
    return;
  }
  if (msg.type === 'pong') return;
}

// ---------------------------------------------------------------------------
// HTTP routing
// ---------------------------------------------------------------------------

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
};

function sendJson(res, status, body) {
  const buf = Buffer.from(JSON.stringify(body));
  res.writeHead(status, { 'content-type': 'application/json; charset=utf-8', 'content-length': buf.length });
  res.end(buf);
}

async function serveStatic(req, res, pathname) {
  const rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const filePath = path.normalize(path.join(WEB_DIR, rel));
  if (!filePath.startsWith(WEB_DIR)) {
    res.writeHead(403);
    return res.end('forbidden');
  }
  try {
    const data = await fsp.readFile(filePath);
    res.writeHead(200, {
      'content-type': MIME[path.extname(filePath)] || 'application/octet-stream',
      'content-length': data.length,
    });
    res.end(data);
  } catch {
    res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
    res.end('not found');
  }
}

function checkSymbol(symbol) {
  if (!symbol || !state.has(symbol)) return `unknown symbol: ${symbol}`;
  return null;
}

function handleBackfill(req, res, url) {
  const symbol = url.searchParams.get('symbol');
  const err = checkSymbol(symbol);
  if (err) return sendJson(res, 404, { error: err });
  const from = Number(url.searchParams.get('from'));
  const to = Number(url.searchParams.get('to'));
  if (!Number.isInteger(from) || !Number.isInteger(to) || from < 1 || to < from || to - from > 1000) {
    return sendJson(res, 400, { error: 'invalid range (need 1 <= from <= to, span <= 1000)' });
  }
  const s = state.get(symbol);
  const earliest = earliestSeq(s);
  const messages = [];
  const missing = [];
  for (let seq = from; seq <= to; seq++) {
    const rec = s.history.get(seq);
    if (rec) messages.push(rec);
    else missing.push(seq);
  }
  if (missing.length) {
    const reason = missing.some((n) => n < s.seq) ? 'expired' : 'not_produced';
    return sendJson(res, 410, {
      error: reason,
      symbol,
      earliestSeq: earliest,
      latestSeq: s.seq,
      missing,
      messages, // whatever still survives, for diagnostics
    });
  }
  return sendJson(res, 200, { symbol, from, to, messages });
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => {
      data += c;
      if (data.length > 1e6) reject(new Error('body too large'));
    });
    req.on('end', () => {
      try {
        resolve(data ? JSON.parse(data) : {});
      } catch {
        reject(new Error('invalid json body'));
      }
    });
    req.on('error', reject);
  });
}

async function handleAdmin(req, res, url) {
  const action = url.pathname.split('/').pop();
  let body = {};
  if (req.method === 'POST') {
    try {
      body = await readBody(req);
    } catch (e) {
      return sendJson(res, 400, { error: e.message });
    }
  }
  const symbol = body.symbol || url.searchParams.get('symbol');

  switch (action) {
    case 'state': {
      const out = { blackhole: globalFlags.blackhole, clients: clients.size, symbols: {} };
      for (const [sym, s] of state.entries()) {
        out.symbols[sym] = { latestSeq: s.seq, earliestSeq: earliestSeq(s), retained: s.history.size };
      }
      return sendJson(res, 200, out);
    }

    case 'disconnect': {
      // hard close: TCP/WS drop, clients must reconnect
      for (const c of clients) {
        try {
          c.socket.write(buildCloseFrame(1001, 'injected disconnect'));
        } catch {
          /* noop */
        }
        c.destroy();
      }
      return sendJson(res, 200, { ok: true, closed: clients.size });
    }

    case 'network': {
      const on = body.mode === 'on';
      globalFlags.blackhole = !on;
      return sendJson(res, 200, { ok: true, blackhole: globalFlags.blackhole });
    }

    case 'duplicate': {
      const err = checkSymbol(symbol);
      if (err) return sendJson(res, 404, { error: err });
      const s = state.get(symbol);
      if (s.seq < 1) return sendJson(res, 409, { error: 'no messages yet' });
      const times = Math.min(10, Math.max(1, Number(body.times) || 1));
      const rec = s.history.get(s.seq);
      for (let i = 0; i < times; i++) broadcast({ ...rec, injected: 'duplicate' });
      return sendJson(res, 200, { ok: true, seq: rec.seq, times });
    }

    case 'reorder': {
      const err = checkSymbol(symbol);
      if (err) return sendJson(res, 404, { error: err });
      const s = state.get(symbol);
      if (s.seq < 2) return sendJson(res, 409, { error: 'not enough messages' });
      const rec = s.history.get(s.seq - 1);
      broadcast({ ...rec, injected: 'reorder' });
      return sendJson(res, 200, { ok: true, seq: rec.seq });
    }

    case 'conflict': {
      const err = checkSymbol(symbol);
      if (err) return sendJson(res, 404, { error: err });
      const s = state.get(symbol);
      if (s.seq < 1) return sendJson(res, 409, { error: 'no messages yet' });
      const original = s.history.get(s.seq);
      // same seq, different content — history keeps the original on purpose
      const forged = {
        ...original,
        price: Number((original.price * 1.07).toFixed(2)),
        ts: Date.now(),
        injected: 'conflict',
      };
      broadcast(forged);
      return sendJson(res, 200, { ok: true, seq: forged.seq, original, forged });
    }

    case 'delete': {
      const err = checkSymbol(symbol);
      if (err) return sendJson(res, 404, { error: err });
      const s = state.get(symbol);
      const from = Number.isInteger(body.from) ? body.from : earliestSeq(s);
      const to = Number.isInteger(body.to) ? body.to : Math.min(s.seq, from + 2);
      if (from < 1 || to < from) return sendJson(res, 400, { error: 'invalid range' });
      const removed = [];
      for (let seq = from; seq <= to; seq++) {
        if (s.history.delete(seq)) removed.push(seq);
      }
      persistSoon();
      return sendJson(res, 200, { ok: true, symbol, removed, earliestSeq: earliestSeq(s), latestSeq: s.seq });
    }

    default:
      return sendJson(res, 404, { error: 'unknown admin action' });
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  try {
    if (url.pathname === '/api/backfill') return handleBackfill(req, res, url);
    if (url.pathname.startsWith('/api/admin/')) return handleAdmin(req, res, url);
    if (url.pathname.startsWith('/api/')) return sendJson(res, 404, { error: 'not found' });
    return await serveStatic(req, res, url.pathname);
  } catch (e) {
    sendJson(res, 500, { error: String(e && e.message || e) });
  }
});

// WebSocket upgrade
server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || req.headers.upgrade.toLowerCase() !== 'websocket') {
    socket.write('HTTP/1.1 400 Bad Request\r\n\r\n');
    return socket.destroy();
  }
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      `Sec-WebSocket-Accept: ${acceptKey(key)}\r\n\r\n`,
  );

  const client = new Client(socket);
  clients.add(client);

  socket.on('data', (chunk) => {
    client.buffer = Buffer.concat([client.buffer, chunk]);
    try {
      const { frames, rest } = parseFrames(client.buffer);
      client.buffer = rest;
      for (const frame of frames) handleFrame(client, frame);
    } catch (e) {
      const code = e instanceof FrameError ? e.code : 1002;
      try {
        socket.write(buildCloseFrame(code, String(e.message).slice(0, 100)));
      } catch {
        /* noop */
      }
      client.destroy();
    }
  });
  socket.on('error', () => client.destroy());
  socket.on('close', () => {
    clients.delete(client);
  });
});

// ---------------------------------------------------------------------------
// Tickers
// ---------------------------------------------------------------------------

loadState();

const tickTimer = setInterval(() => {
  for (const { symbol } of SYMBOLS) {
    const record = produceTick(symbol);
    broadcast(record);
  }
}, TICK_MS);

// app-level heartbeat: any incoming frame proves liveness to the client
const pingTimer = setInterval(() => broadcast({ type: 'ping', ts: Date.now() }), 5000);

// Don't keep the process alive solely on the tickers (important for tests).
tickTimer.unref();
pingTimer.unref();

server.listen(PORT, () => {
  console.log(`quote service listening on http://localhost:${PORT}`);
  console.log(`symbols: ${SYMBOLS.map((x) => x.symbol).join(', ')} | retention ${RETENTION} | tick ${TICK_MS}ms`);
});

function shutdown() {
  clearInterval(tickTimer);
  clearInterval(pingTimer);
  clearTimeout(saveTimer);
  server.close();
}

module.exports = { server, state, SYMBOLS, RETENTION, shutdown };
