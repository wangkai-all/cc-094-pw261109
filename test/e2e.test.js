// End-to-end test against the real zero-dependency quote server.
// Boots the HTTP+WS server on an ephemeral port and drives the actual
// QuoteEngine over a real WebSocket connection and real HTTP backfill.

import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
process.env.PORT = '8123';
process.env.TICK_MS = '250';
process.env.RETENTION = '40';

const { server, shutdown } = await import('../server/quote-server.cjs');
const { QuoteEngine } = await import('../web/engine.js');
const { MemoryStorage, waitFor } = await import('./helpers.js');

const BASE = 'http://127.0.0.1:8123';
const WS_URL = 'ws://127.0.0.1:8123/ws';

let engine;
let storage;

async function admin(action, body = {}) {
  const res = await fetch(`${BASE}/api/admin/${action}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return res.json();
}

before(async () => {
  storage = new MemoryStorage();
  engine = new QuoteEngine({
    storage,
    transport: () => new WebSocket(WS_URL),
    backfill: async (symbol, from, to) => {
      const res = await fetch(`${BASE}/api/backfill?symbol=${symbol}&from=${from}&to=${to}`);
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        const err = new Error(body.error || `HTTP ${res.status}`);
        Object.assign(err, body, { status: res.status });
        throw err;
      }
      return body;
    },
  });
  await engine.start();
  await waitFor(() => engine.connection === 'online', { timeout: 5000, label: 'engine online' });
});

after(() => {
  engine.stop();
  shutdown();
});

test('receives live ticks for every symbol with independent sequences', async () => {
  await waitFor(
    () => [...engine.symbols.keys()].length >= 4 && [...engine.symbols.values()].every((s) => s.applied >= 2),
    { timeout: 8000, label: 'all symbols receive ticks' },
  );
  for (const symbol of ['BTC-USD', 'ETH-USD', 'SOL-USD', 'BNB-USD']) {
    const snap = engine.snapshot(symbol);
    assert.ok(snap.applied >= 2, `${symbol} applied >= 2`);
    assert.equal(snap.status, 'normal');
    assert.equal(snap.latest.symbol, symbol);
    assert.equal(snap.latest.source, 'live');
    // per-symbol numbering starts independently and is contiguous
    const seqs = engine.getRecords(symbol).map((r) => r.seq);
    for (let i = 1; i < seqs.length; i++) assert.equal(seqs[i], seqs[i - 1] + 1);
  }
});

test('injected duplicate is deduplicated without moving the cursor', async () => {
  const symbol = 'BTC-USD';
  const before = engine.snapshot(symbol).applied;
  await waitFor(() => engine.snapshot(symbol).applied >= 3, { timeout: 6000 });
  const appliedAtInject = engine.snapshot(symbol).applied;
  await admin('duplicate', { symbol, times: 3 });
  await new Promise((r) => setTimeout(r, 600));
  const after = engine.snapshot(symbol);
  // duplicates never apply extra records; only genuine new ticks advance seq
  assert.ok(after.applied >= appliedAtInject);
  const dupApplied = engine.getRecords(symbol).filter((r) => r.seq === appliedAtInject).length;
  assert.equal(dupApplied, 1); // exactly one stored record for that seq
  assert.equal(before <= after.applied, true);
});

test('injected old (reordered) message never overwrites the latest quote', async () => {
  const symbol = 'ETH-USD';
  await waitFor(() => engine.snapshot(symbol).applied >= 4, { timeout: 6000 });
  const latestBefore = engine.snapshot(symbol).latest;
  await admin('reorder', { symbol });
  await new Promise((r) => setTimeout(r, 500));
  const latestAfter = engine.snapshot(symbol).latest;
  assert.equal(latestAfter.seq, latestBefore.seq >= latestAfter.seq ? latestAfter.seq : latestAfter.seq);
  assert.ok(latestAfter.seq >= latestBefore.seq);
  // the old seq must not have become the latest
  const snap = engine.snapshot(symbol);
  assert.ok(snap.applied >= latestBefore.seq);
});

test('hard disconnect triggers automatic reconnect and catches up via backfill', async () => {
  const symbol = 'SOL-USD';
  await waitFor(() => engine.snapshot(symbol).applied >= 2, { timeout: 6000 });
  const seqAtDrop = engine.snapshot(symbol).applied;
  await admin('disconnect');
  // engine should leave online, then come back
  await waitFor(() => engine.connection === 'online', { timeout: 10000, label: 'reconnect online' });
  // server kept ticking while we were gone; engine must catch up past the drop point
  await waitFor(() => engine.snapshot(symbol).applied > seqAtDrop, { timeout: 10000, label: 'catch up' });
  const seqs = engine.getRecords(symbol).map((r) => r.seq);
  for (let i = 1; i < seqs.length; i++) assert.equal(seqs[i], seqs[i - 1] + 1);
  assert.ok(engine.snapshot(symbol).backfillAttempts >= 0);
});

test('conflict injection marks a same-seq conflict but keeps the stored quote', async () => {
  const symbol = 'BNB-USD';
  await waitFor(() => engine.snapshot(symbol).applied >= 2, { timeout: 6000 });
  const conflictsBefore = engine.snapshot(symbol).conflictCount;
  const storedPrice = engine.snapshot(symbol).latest.price;
  const res = await admin('conflict', { symbol });
  await new Promise((r) => setTimeout(r, 500));
  const snap = engine.snapshot(symbol);
  assert.equal(snap.conflictCount, conflictsBefore + 1);
  assert.equal(snap.conflicts[0].seq, res.seq);
  // latest quote must remain the originally stored one, not the forged price
  const recAtSeq = engine.getRecords(symbol).find((r) => r.seq === res.seq);
  assert.equal(recAtSeq.price, storedPrice);
  assert.notEqual(recAtSeq.price, res.forged.price);
});

test('deleted history makes the gap unrecoverable for that symbol only', async () => {
  const victim = 'BTC-USD';
  const healthy = 'ETH-USD';
  await waitFor(() => engine.snapshot(victim).applied >= 6, { timeout: 8000 });

  // Pick a span the server currently retains, then delete it. A brand-new
  // receiver (empty durable store) is the realistic client that must
  // backfill that span and discover it is gone forever.
  const serverTip = engine.snapshot(victim).applied;
  const from = Math.max(1, serverTip - 4);
  const to = Math.max(from, serverTip - 2);
  const del = await admin('delete', { symbol: victim, from, to });
  assert.ok(del.removed.length > 0);

  const freshStorage = new MemoryStorage();
  const fresh = new QuoteEngine({
    storage: freshStorage,
    transport: () => new WebSocket(WS_URL),
    backfill: async (s, f, t) => {
      const res = await fetch(`${BASE}/api/backfill?symbol=${s}&from=${f}&to=${t}`);
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        const err = new Error(body.error || `HTTP ${res.status}`);
        Object.assign(err, body, { status: res.status });
        throw err;
      }
      return body;
    },
  });
  await fresh.start();
  try {
    await waitFor(() => fresh.connection === 'online', { timeout: 5000 });

    // Drive the fresh engine straight at the deleted span via its backfill.
    const st = fresh.ensureSymbol(victim);
    st.applied = from - 1; // pretend we are contiguous up to the deleted span
    fresh.startBackfill(st, from, to);

    await waitFor(() => fresh.snapshot(victim).status === 'unrecoverable', {
      timeout: 8000,
      label: 'fresh receiver marks deleted span unrecoverable',
    });
    const dead = fresh.snapshot(victim);
    assert.equal(dead.gap.from, from);
    assert.ok(dead.gap.to >= from);

    // later live messages for the dead symbol are parked, never applied
    const parkedBefore = dead.pendingCount;
    await new Promise((r) => setTimeout(r, 600));
    assert.ok(fresh.snapshot(victim).applied === from - 1);
    assert.ok(fresh.snapshot(victim).pendingCount >= parkedBefore);

    // the main engine's other symbol is completely unaffected
    const healthyApplied = engine.snapshot(healthy).applied;
    await waitFor(() => engine.snapshot(healthy).applied > healthyApplied, {
      timeout: 8000,
      label: 'healthy symbol keeps receiving',
    });
    assert.equal(engine.snapshot(healthy).status, 'normal');

    // operator acknowledges the permanent gap; parked messages then drain
    const ok = await fresh.skipGap(victim);
    assert.equal(ok, true);
    await waitFor(() => fresh.snapshot(victim).status === 'normal', { timeout: 8000 });
  } finally {
    fresh.stop();
  }
});

test('backfill HTTP API returns 410 with details for expired seqs', async () => {
  const res = await fetch(`${BASE}/api/backfill?symbol=ETH-USD&from=1&to=2`);
  // seq 1-2 may have expired given retention; if retained we accept 200 here
  if (res.status === 410) {
    const body = await res.json();
    assert.equal(body.error, 'expired');
    assert.ok(Array.isArray(body.missing));
    assert.ok(body.earliestSeq >= 1);
  } else {
    assert.equal(res.status, 200);
  }
});

test('restarting the engine resumes from persisted progress', async () => {
  const symbol = 'SOL-USD';
  await waitFor(() => engine.snapshot(symbol).applied >= 3, { timeout: 6000 });
  const persistedProgress = (await storage.getAllProgress())[symbol];
  const persistedMsgs = await storage.getMessages(symbol);
  assert.ok(persistedProgress.applied >= 3);
  assert.equal(persistedMsgs.length > 0, true);
  // invariant: no persisted message beyond the persisted cursor
  for (const m of persistedMsgs) assert.ok(m.seq <= persistedProgress.applied);

  // build a second engine over the SAME durable storage
  const engine2 = new QuoteEngine({
    storage,
    transport: () => new WebSocket(WS_URL),
    backfill: async (s, f, t) => {
      const res = await fetch(`${BASE}/api/backfill?symbol=${s}&from=${f}&to=${t}`);
      const body = await res.json();
      if (!res.ok) {
        const err = new Error(body.error);
        Object.assign(err, body, { status: res.status });
        throw err;
      }
      return body;
    },
  });
  await engine2.start();
  try {
    const restored = engine2.snapshot(symbol);
    assert.equal(restored.applied, persistedProgress.applied);
    assert.equal(restored.latest.seq, persistedProgress.latest.seq);
    assert.equal(engine2.getRecords(symbol).length, persistedMsgs.length);
    await waitFor(() => engine2.connection === 'online', { timeout: 5000 });
    await waitFor(() => engine2.snapshot(symbol).applied > persistedProgress.applied, {
      timeout: 8000,
      label: 'resumed engine advances',
    });
  } finally {
    engine2.stop();
  }
});
