import { test } from 'node:test';
import assert from 'node:assert/strict';
import { QuoteEngine, EVENT } from '../web/engine.js';
import { MemoryStorage, FakeSocket, quote, waitFor } from './helpers.js';

function makeEngine(backfillImpl) {
  const storage = new MemoryStorage();
  const sockets = [];
  const engine = new QuoteEngine({
    storage,
    transport: () => {
      const sock = new FakeSocket();
      sockets.push(sock);
      return sock;
    },
    backfill:
      backfillImpl ||
      (async () => {
        throw new Error('backfill not expected in this test');
      }),
  });
  return { engine, storage, sockets };
}

async function startEngine(engine, sockets, positions = {}) {
  const started = engine.start();
  await waitFor(() => sockets[0]);
  sockets[0].open();
  await waitFor(() => sockets[0].sent.some((m) => m.type === 'hello'));
  sockets[0].deliver({ type: 'hello_ack', positions });
  await waitFor(() => engine.connection === 'online');
  await started;
}

test('in-order live messages apply sequentially and persist together with progress', async () => {
  const { engine, storage } = makeEngine();
  const applied = [];
  engine.addEventListener(EVENT.APPLIED, (e) => applied.push(e.detail.record.seq));

  // first message anchors an empty subscriber
  await engine.ingest(quote('BTC-USD', 5), 'live');
  await engine.ingest(quote('BTC-USD', 6), 'live');
  await engine.ingest(quote('BTC-USD', 7), 'live');

  assert.equal(engine.snapshot('BTC-USD').applied, 7);
  assert.deepEqual(applied, [5, 6, 7]);
  const progress = await storage.getAllProgress();
  assert.equal(progress['BTC-USD'].applied, 7);
  const msgs = await storage.getMessages('BTC-USD');
  assert.deepEqual(msgs.map((m) => m.seq), [5, 6, 7]);
  // each applied record is persisted atomically with its cursor
  for (const m of msgs) assert.equal(m.source, m.seq === 5 ? 'anchor' : 'live');
});

test('duplicate and old messages never overwrite the latest quote', async () => {
  const { engine } = makeEngine();
  await engine.ingest(quote('BTC-USD', 1), 'live');
  await engine.ingest(quote('BTC-USD', 2), 'live');
  await engine.ingest(quote('BTC-USD', 3), 'live');

  const before = engine.snapshot('BTC-USD').latest;
  await engine.ingest(quote('BTC-USD', 1, { price: 999 }), 'live'); // old seq, different content
  await engine.ingest(quote('BTC-USD', 2), 'live'); // exact duplicate, no conflict
  await engine.ingest(quote('BTC-USD', 3, { price: 888 }), 'live'); // old seq conflict

  const snap = engine.snapshot('BTC-USD');
  assert.equal(snap.applied, 3);
  assert.equal(snap.latest.price, before.price);
  assert.equal(snap.conflictCount, 2); // seq 1 and seq 3 content differed, seq 2 identical
  assert.equal(snap.conflicts[0].seq, 3);
});

test('gap parks later messages, backfill fills it, then pending drains in order', async () => {
  const backfillCalls = [];
  const { engine } = makeEngine(async (symbol, from, to) => {
    backfillCalls.push([from, to]);
    const messages = [];
    for (let seq = from; seq <= to; seq++) messages.push(quote(symbol, seq));
    return { messages };
  });

  await engine.ingest(quote('ETH-USD', 10), 'live');
  await engine.ingest(quote('ETH-USD', 14), 'live'); // gap 11-13 -> parked
  const snap1 = engine.snapshot('ETH-USD');
  assert.equal(snap1.applied, 10);
  assert.deepEqual(snap1.pendingSeqs, [14]);
  assert.ok(snap1.backfilling || snap1.backfillAttempts >= 1);

  await waitFor(() => engine.snapshot('ETH-USD').applied === 14);
  assert.deepEqual(backfillCalls, [[11, 13]]);
  const snap2 = engine.snapshot('ETH-USD');
  assert.equal(snap2.pendingCount, 0);
  assert.equal(snap2.backfillAttempts, 1);
  const sources = engine.getRecords('ETH-USD').map((r) => `${r.seq}:${r.source}`);
  assert.deepEqual(sources, ['10:anchor', '11:backfill', '12:backfill', '13:backfill', '14:live']);
});

test('live/backfill overlap is processed once with conflict detection', async () => {
  let resolveBf;
  const { engine } = makeEngine(
    () =>
      new Promise((resolve) => {
        resolveBf = resolve;
      }),
  );
  await engine.ingest(quote('SOL-USD', 1), 'live');
  await engine.ingest(quote('SOL-USD', 4), 'live'); // gap 2-3, backfill pending
  assert.equal(engine.snapshot('SOL-USD').backfilling, true);

  // while backfill is in flight, the socket delivers seq 2 (overlap) and 5
  await engine.ingest(quote('SOL-USD', 2), 'live');
  await engine.ingest(quote('SOL-USD', 5), 'live');

  resolveBf({
    messages: [
      quote('SOL-USD', 2), // identical content -> dedup, no conflict
      quote('SOL-USD', 3, { price: 303.33 }),
    ],
  });

  await waitFor(() => engine.snapshot('SOL-USD').applied === 5);
  const snap = engine.snapshot('SOL-USD');
  assert.equal(snap.conflictCount, 0);
  assert.equal(snap.pendingCount, 0);
  assert.deepEqual(engine.getRecords('SOL-USD').map((r) => r.seq), [1, 2, 3, 4, 5]);
  assert.equal(engine.getRecords('SOL-USD').find((r) => r.seq === 2).source, 'backfill');
});

test('overlapping same-seq content different between live and backfill raises conflict', async () => {
  let resolveBf;
  const { engine } = makeEngine(
    () =>
      new Promise((resolve) => {
        resolveBf = resolve;
      }),
  );
  await engine.ingest(quote('SOL-USD', 1), 'live');
  await engine.ingest(quote('SOL-USD', 3), 'live');
  await engine.ingest(quote('SOL-USD', 2, { price: 1200 }), 'live'); // different from backfill's

  resolveBf({ messages: [quote('SOL-USD', 2, { price: 102 })] });

  await waitFor(() => engine.snapshot('SOL-USD').backfilling === false);
  const snap = engine.snapshot('SOL-USD');
  assert.equal(snap.applied, 3);
  assert.equal(snap.conflictCount, 1);
  assert.equal(snap.conflicts[0].seq, 2);
  // applied value comes from backfill (the authoritative gap fill), conflict is recorded
  assert.equal(engine.getRecords('SOL-USD').find((r) => r.seq === 2).price, 102);
});

test('expired backfill marks symbol unrecoverable but other symbols keep receiving', async () => {
  const { engine } = makeEngine(async () => {
    const err = new Error('expired');
    err.status = 410;
    err.earliestSeq = 15;
    throw err;
  });

  await engine.ingest(quote('BNB-USD', 10), 'live');
  await engine.ingest(quote('BNB-USD', 13), 'live'); // gap 11-12, backfill 410
  await waitFor(() => engine.snapshot('BNB-USD').status === 'unrecoverable');

  const dead = engine.snapshot('BNB-USD');
  assert.deepEqual([dead.gap.from, dead.gap.to], [11, 12]);
  assert.equal(dead.applied, 10); // latest quote frozen

  // messages after the dead gap are parked, never applied over the freeze
  await engine.ingest(quote('BNB-USD', 14), 'live');
  assert.equal(engine.snapshot('BNB-USD').applied, 10);
  assert.deepEqual(engine.snapshot('BNB-USD').pendingSeqs, [13, 14]);

  // a different symbol is completely unaffected
  await engine.ingest(quote('ETH-USD', 20), 'live');
  await engine.ingest(quote('ETH-USD', 21), 'live');
  assert.equal(engine.snapshot('ETH-USD').status, 'normal');
  assert.equal(engine.snapshot('ETH-USD').applied, 21);

  // operator acknowledges the gap -> parked messages drain
  assert.equal(await engine.skipGap('BNB-USD'), true);
  await waitFor(() => engine.snapshot('BNB-USD').applied === 14);
  assert.equal(engine.snapshot('BNB-USD').status, 'normal');
});

test('unrecoverable state is persisted and survives engine restart', async () => {
  const backfills = {
    first: async () => {
      const err = new Error('expired');
      err.status = 410;
      err.earliestSeq = 9;
      throw err;
    },
  };
  const storage = new MemoryStorage();
  const makeOpts = () => ({
    storage,
    transport: () => new FakeSocket(),
    backfill: (s, f, t) => backfills.first(s, f, t),
  });
  const e1 = new QuoteEngine(makeOpts());
  await e1.ingest(quote('BTC-USD', 5), 'live');
  await e1.ingest(quote('BTC-USD', 8), 'live');
  await waitFor(() => e1.snapshot('BTC-USD').status === 'unrecoverable');
  e1.stop();

  // restart with the same durable storage
  const e2 = new QuoteEngine(makeOpts());
  await e2.start();
  const snap = e2.snapshot('BTC-USD');
  assert.equal(snap.status, 'unrecoverable');
  assert.deepEqual([snap.gap.from, snap.gap.to], [6, 7]);
  assert.equal(snap.applied, 5);
  assert.equal(snap.latest.price, 105);
  e2.stop();
});

test('transient backfill failure retries and then succeeds', async () => {
  let calls = 0;
  const { engine } = makeEngine(async (symbol, from, to) => {
    calls += 1;
    if (calls === 1) throw new Error('network reset');
    const messages = [];
    for (let seq = from; seq <= to; seq++) messages.push(quote(symbol, seq));
    return { messages };
  });
  await engine.ingest(quote('BTC-USD', 1), 'live');
  await engine.ingest(quote('BTC-USD', 3), 'live');
  await waitFor(() => engine.snapshot('BTC-USD').applied === 3, { timeout: 8000 });
  assert.ok(calls >= 2);
  assert.equal(engine.snapshot('BTC-USD').status, 'normal');
});

test('failed storage transaction does not advance the in-memory cursor', async () => {
  const { engine, storage } = makeEngine();
  await engine.ingest(quote('BTC-USD', 1), 'live');
  storage.failOnce = 'put';
  await assert.rejects(() => engine.ingest(quote('BTC-USD', 2), 'live'), /simulated transaction failure/);
  // cursor stays at 1; the failed seq can be re-delivered and then applied
  assert.equal(engine.snapshot('BTC-USD').applied, 1);
  await engine.ingest(quote('BTC-USD', 2), 'live');
  assert.equal(engine.snapshot('BTC-USD').applied, 2);
  const progress = await storage.getAllProgress();
  assert.equal(progress['BTC-USD'].applied, 2);
});

test('reconnect hello with server tip ahead triggers backfill for the missed span', async () => {
  const calls = [];
  const { engine, sockets } = makeEngine(async (symbol, from, to) => {
    calls.push([from, to]);
    return { messages: [quote(symbol, from), quote(symbol, from + 1)].slice(0, to - from + 1) };
  });
  await startEngine(engine, sockets, { 'BTC-USD': { latestSeq: 4, earliestSeq: 1 } });
  await engine.ingest(quote('BTC-USD', 4), 'live');
  assert.equal(engine.snapshot('BTC-USD').applied, 4);

  // socket drops, reconnects, server says it moved to 6
  sockets[0].close();
  await waitFor(() => sockets.length === 2);
  sockets[1].open();
  sockets[1].deliver({ type: 'hello_ack', positions: { 'BTC-USD': { latestSeq: 6, earliestSeq: 1 } } });

  await waitFor(() => engine.snapshot('BTC-USD').applied === 6);
  assert.deepEqual(calls, [[5, 6]]);
  engine.stop();
});

test('per-symbol sequences are independent and a conflict on one does not block others', async () => {
  const { engine } = makeEngine();
  await engine.ingest(quote('BTC-USD', 1), 'live');
  await engine.ingest(quote('ETH-USD', 1), 'live');
  await engine.ingest(quote('BTC-USD', 1, { price: 1 }), 'live'); // conflict on BTC only
  assert.equal(engine.snapshot('BTC-USD').conflictCount, 1);
  assert.equal(engine.snapshot('ETH-USD').conflictCount, 0);
  await engine.ingest(quote('ETH-USD', 2), 'live');
  assert.equal(engine.snapshot('ETH-USD').applied, 2);
});

test('rewindSymbol atomically rolls back records+progress and the expired backfill marks unrecoverable', async () => {
  const { engine, storage } = makeEngine(async () => {
    const err = new Error('expired');
    err.status = 410;
    err.earliestSeq = 99;
    throw err;
  });
  await engine.ingest(quote('BTC-USD', 1), 'live');
  await engine.ingest(quote('BTC-USD', 2), 'live');
  await engine.ingest(quote('BTC-USD', 3), 'live');
  await engine.ingest(quote('BTC-USD', 4), 'live');
  assert.equal(engine.snapshot('BTC-USD').applied, 4);

  const ok = await engine.rewindSymbol('BTC-USD', 2);
  assert.equal(ok, true);
  const snap = engine.snapshot('BTC-USD');
  assert.equal(snap.applied, 2); // cursor rewound
  assert.equal(snap.recordCount, 2); // only seq 1-2 remain
  const persisted = await storage.getAllProgress();
  assert.equal(persisted['BTC-USD'].applied, 2);
  const msgs = await storage.getMessages('BTC-USD');
  assert.deepEqual(msgs.map((m) => m.seq), [1, 2]);

  await waitFor(() => engine.snapshot('BTC-USD').status === 'unrecoverable');
  const dead = engine.snapshot('BTC-USD');
  assert.deepEqual([dead.gap.from, dead.gap.to], [3, 4]);
});
