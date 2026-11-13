// Sequential quote engine — DOM-free, usable in browser and Node tests.
//
// Guarantees per symbol:
//   1. Latest quote only moves forward in sequence; late/old messages never
//      overwrite it (incoming seq <= applied is discarded after conflict check).
//   2. A detected gap parks later messages in `pending`; an HTTP backfill
//      fills the gap, then parked messages drain in strict seq order.
//   3. Live messages and backfill results overlap -> processed exactly once
//      (pending keyed by seq, backfill never re-applies applied seqs).
//   4. Same seq, different content -> conflict recorded and surfaced, never
//      silently resolved.
//   5. Expired backfill -> symbol marked unrecoverable; other symbols keep
//      receiving normally.
//   6. Every applied record is persisted atomically together with the progress
//      cursor (single transaction); the cursor never advances before the
//      message is durably stored.

export const EVENT = Object.freeze({
  APPLIED: 'applied',
  GAP: 'gap',
  BACKFILL_START: 'backfill-start',
  BACKFILL_DONE: 'backfill-done',
  UNRECOVERABLE: 'unrecoverable',
  GAP_SKIPPED: 'gap-skipped',
  CONFLICT: 'conflict',
  CONNECTION: 'connection',
  SYMBOL: 'symbol',
});

const MAX_RECORDS = 400; // per-symbol durable ring
const MAX_CONFLICTS = 50;
const BACKFILL_CHUNK = 200;
const MAX_BACKOFF = 8000;
const HELLO_TIMEOUT = 6000;

// Fields that define quote content. Storage-only metadata (source, reason,
// receivedAt) and injection tags are excluded from conflict comparison.
function canonical(rec) {
  if (!rec) return 'null';
  const picked = {};
  for (const k of ['symbol', 'seq', 'ts', 'price', 'bid', 'ask', 'volume']) picked[k] = rec[k];
  return JSON.stringify(picked);
}

export class QuoteEngine extends EventTarget {
  /**
   * @param {object} opts
   * @param {StorageLike} opts.storage   atomic persistence adapter
   * @param {TransportLike} opts.transport  connect() -> WebSocket-like
   * @param {(symbol: string, from: number, to: number) => Promise<BackfillResult>} opts.backfill
   * @param {() => number} [opts.now]
   */
  constructor({ storage, transport, backfill, now = () => Date.now() }) {
    super();
    this.storage = storage;
    this.transport = transport;
    this.backfillFn = backfill;
    this.now = now;
    this.symbols = new Map(); // symbol -> state
    this.ws = null;
    this.stopped = false; // stop() flips this to cancel reconnect/retry timers
    this.reconnectTimer = null;
    this.helloTimer = null;
    this.attempt = 0;
    this.connection = 'stopped';
  }

  emit(type, detail) {
    this.dispatchEvent(new CustomEvent(type, { detail }));
  }

  setConnection(status) {
    if (this.connection === status) return;
    this.connection = status;
    this.emit(EVENT.CONNECTION, { status });
  }

  // -- lifecycle -----------------------------------------------------------

  async start() {
    await this.storage.init();
    const saved = await this.storage.getAllProgress();
    for (const [symbol, progress] of Object.entries(saved || {})) {
      const records = await this.storage.getMessages(symbol);
      this.symbols.set(symbol, this.hydrate(progress, records));
      this.emit(EVENT.SYMBOL, { symbol, state: this.snapshot(symbol) });
    }
    this.stopped = false;
    this.attempt = 0;
    this.connect();
  }

  stop() {
    this.stopped = true;
    clearTimeout(this.reconnectTimer);
    clearTimeout(this.helloTimer);
    if (this.ws) {
      try {
        this.ws.close(1000, 'client stop');
      } catch {
        /* noop */
      }
    }
    this.ws = null;
    this.setConnection('stopped');
  }

  hydrate(progress, records) {
    const map = new Map(records.map((r) => [r.seq, r]));
    return {
      symbol: progress.symbol,
      applied: progress.applied,
      latest: progress.latest || null,
      conflicts: progress.conflicts || [],
      conflictCount: progress.conflictCount || 0,
      status: progress.status || 'normal', // normal | unrecoverable
      gap: progress.gap || null, // { from, to } permanently-missing range (persisted)
      records: map, // seq -> stored record (in-memory mirror of durable store)
      pending: new Map(), // seq -> parked live record (not yet durable)
      inflight: null, // { from, to } backfill range currently requested
      backfilling: false,
      backfillAttempts: 0,
      lastError: null,
    };
  }

  ensureSymbol(symbol) {
    let st = this.symbols.get(symbol);
    if (st) return st;
    st = this.hydrate(
      { symbol, applied: 0, latest: null, conflicts: [], conflictCount: 0, status: 'normal', gap: null },
      [],
    );
    this.symbols.set(symbol, st);
    this.emit(EVENT.SYMBOL, { symbol, state: this.snapshot(symbol) });
    return st;
  }

  connect() {
    if (this.stopped) return;
    this.setConnection('connecting');
    let ws;
    try {
      ws = this.transport();
    } catch (e) {
      return this.scheduleReconnect(e);
    }
    this.ws = ws;

    const helloTimer = setTimeout(() => {
      this.setConnection('degraded');
      try {
        ws.close();
      } catch {
        /* noop */
      }
    }, HELLO_TIMEOUT);
    this.helloTimer = helloTimer;

    ws.onopen = () => {
      const cursors = {};
      for (const [symbol, st] of this.symbols.entries()) cursors[symbol] = st.applied;
      ws.send(JSON.stringify({ type: 'hello', cursors }));
    };

    ws.onmessage = (evt) => {
      let msg;
      try {
        msg = JSON.parse(typeof evt.data === 'string' ? evt.data : evt.data.toString());
      } catch {
        return; // ignore malformed frames
      }
      this.onMessage(msg);
    };

    ws.onclose = () => {
      clearTimeout(helloTimer);
      if (this.stopped) return;
      this.scheduleReconnect();
    };
    ws.onerror = () => {
      try {
        ws.close();
      } catch {
        /* noop */
      }
    };
  }

  scheduleReconnect(err) {
    if (this.stopped) return;
    this.setConnection('reconnecting');
    this.attempt += 1;
    const delay = Math.min(MAX_BACKOFF, 250 * 2 ** Math.min(this.attempt, 6)) + Math.random() * 200;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => this.connect(), delay);
    if (err) {
      for (const st of this.symbols.values()) st.lastError = String(err.message || err);
    }
  }

  // -- message handling ----------------------------------------------------

  onMessage(msg) {
    if (msg.type === 'hello_ack') {
      clearTimeout(this.helloTimer);
      this.attempt = 0;
      this.setConnection('online');
      this.onPositions(msg.positions || {});
      return;
    }
    if (msg.type === 'ping') {
      if (this.ws && this.ws.readyState === 1) {
        try {
          this.ws.send(JSON.stringify({ type: 'pong', ts: this.now() }));
        } catch {
          /* noop */
        }
      }
      return;
    }
    if (msg.type !== 'quote' || !msg.symbol || !Number.isInteger(msg.seq)) return;
    this.ingest(msg, 'live').catch((err) => {
      const st = this.symbols.get(msg.symbol);
      if (st) st.lastError = String(err && err.message || err);
    });
  }

  onPositions(positions) {
    for (const [symbol, pos] of Object.entries(positions)) {
      const st = this.ensureSymbol(symbol);
      st.tip = pos.latestSeq;
      // Reconnect check: server moved ahead while we have no parked message yet.
      if (
        st.applied > 0 &&
        st.status === 'normal' &&
        !st.backfilling &&
        pos.latestSeq > st.applied &&
        st.pending.size === 0
      ) {
        this.startBackfill(st, st.applied + 1, pos.latestSeq);
      }
    }
  }

  /**
   * Ingest one record arriving from either the live socket or a backfill reply.
   * @param source 'live' | 'backfill'
   */
  async ingest(rec, source) {
    const st = this.ensureSymbol(rec.symbol);
    const { seq } = rec;

    // 1) Already applied -> duplicate or conflict, never overwrites latest.
    if (seq <= st.applied) {
      const stored = st.records.get(seq);
      if (stored && canonical(stored) !== canonical(rec)) {
        this.recordConflict(st, { seq, stored, incoming: rec, source });
      }
      return false;
    }

    // 2) Fresh subscriber (nothing applied yet): anchor durably at the first
    //    record we see instead of replaying history we never asked for.
    if (st.applied === 0 && !st.backfilling && st.pending.size === 0) {
      await this.apply(st, rec, 'anchor');
      return true;
    }

    // 3) Unrecoverable gap unresolved: park everything for later draining
    //    (skipGap) without touching the latest quote.
    if (st.status === 'unrecoverable') {
      if (!st.pending.has(seq)) st.pending.set(seq, { ...rec, __source: source });
      return false;
    }

    // 4) Expected next seq -> apply, then drain anything parked behind it.
    if (seq === st.applied + 1 && !st.backfilling) {
      await this.apply(st, rec, source);
      await this.drain(st);
      return true;
    }

    // 5) Future seq -> park; detect gap and start backfill.
    if (seq > st.applied + 1) {
      if (!st.pending.has(seq)) st.pending.set(seq, { ...rec, __source: source });
      if (!st.backfilling) {
        const firstParked = Math.min(...[...st.pending.keys()].filter((n) => n > st.applied));
        if (firstParked > st.applied + 1) {
          this.startBackfill(st, st.applied + 1, firstParked - 1);
        }
      }
      return false;
    }

    // seq inside an active backfill window arriving live -> park, dedup later.
    if (st.backfilling) {
      st.pending.set(seq, { ...rec, __source: source });
      return false;
    }
    return false;
  }

  // -- apply / drain -------------------------------------------------------

  async apply(st, rec, source) {
    const seq = rec.seq;
    const storedRec = {
      symbol: rec.symbol,
      seq,
      ts: rec.ts,
      price: rec.price,
      bid: rec.bid,
      ask: rec.ask,
      volume: rec.volume,
      source,
      reason:
        source === 'backfill'
          ? `补查生效：序号 ${seq} 经 HTTP 补查返回，按序补齐缺口`
          : source === 'anchor'
            ? `定位生效：新订阅锚定首条收到的序号 ${seq}，更早历史不回放`
            : `实时生效：序号 ${seq} 连续到达，刷新最新报价`,
      receivedAt: this.now(),
    };
    if (rec.injected) storedRec.injected = rec.injected;

    // Atomic: record and progress move together, or neither does. Await the
    // transaction so the in-memory cursor never advances on a failed write.
    await this.storage.put(st.symbol, storedRec, {
      ...this.progressOf(st),
      applied: seq,
      latest: storedRec,
    });

    st.records.set(seq, storedRec);
    st.applied = seq;
    st.latest = storedRec;
    this.trimRecords(st);

    this.emit(EVENT.APPLIED, { symbol: st.symbol, record: storedRec, state: this.snapshot(st.symbol) });
  }

  trimRecords(st) {
    if (st.records.size <= MAX_RECORDS) return;
    const seqs = [...st.records.keys()].sort((a, b) => a - b);
    const drop = seqs.slice(0, st.records.size - MAX_RECORDS);
    for (const seq of drop) {
      st.records.delete(seq);
      this.storage.deleteMessage(st.symbol, seq);
    }
  }

  async drain(st) {
    while (st.status === 'normal' && !st.backfilling) {
      const next = st.applied + 1;
      const parked = st.pending.get(next);
      if (!parked) break;
      st.pending.delete(next);
      await this.apply(st, parked, parked.__source || 'live');
    }
  }

  // -- backfill ------------------------------------------------------------

  startBackfill(st, from, to) {
    if (st.backfilling || st.status === 'unrecoverable') return;
    to = Math.min(to, from + BACKFILL_CHUNK - 1);
    st.backfilling = true;
    st.backfillAttempts += 1;
    st.inflight = { from, to };
    this.emit(EVENT.BACKFILL_START, {
      symbol: st.symbol,
      from,
      to,
      attempt: st.backfillAttempts,
      state: this.snapshot(st.symbol),
    });

    this.backfillFn(st.symbol, from, to)
      .then((res) => this.onBackfillOk(st, from, to, res))
      .catch((err) => this.onBackfillError(st, from, to, err));
  }

  async onBackfillOk(st, from, to, res) {
    if (!st.backfilling || !st.inflight || st.inflight.from !== from) return; // stale
    st.backfilling = false;
    st.inflight = null;

    // Merge backfilled records with any live messages that overlapped.
    const bySeq = new Map();
    for (const rec of res.messages || []) bySeq.set(rec.seq, rec);
    for (const [seq, parked] of st.pending.entries()) {
      if (seq < from || seq > to) continue;
      const bf = bySeq.get(seq);
      if (bf) {
        // Overlap: same seq from socket and backfill — compare, keep one.
        if (canonical(bf) !== canonical(parked)) {
          this.recordConflict(st, { seq, stored: bf, incoming: parked, source: 'live-overlap' });
        }
      } else {
        bySeq.set(seq, parked); // backfill missed it but the socket delivered it
      }
      st.pending.delete(seq);
    }

    // Verify completeness against the requested range.
    const missing = [];
    for (let seq = from; seq <= to; seq++) if (!bySeq.has(seq)) missing.push(seq);

    // Apply everything that is contiguous from the cursor; park the rest.
    for (let seq = from; seq <= to; seq++) {
      const rec = bySeq.get(seq);
      if (!rec) continue;
      if (seq === st.applied + 1) {
        await this.apply(st, rec, 'backfill');
      } else if (seq > st.applied + 1) {
        st.pending.set(seq, rec);
      } else {
        const stored = st.records.get(seq);
        if (stored && canonical(stored) !== canonical(rec)) {
          this.recordConflict(st, { seq, stored, incoming: rec, source: 'backfill-overlap' });
        }
      }
    }

    this.emit(EVENT.BACKFILL_DONE, {
      symbol: st.symbol,
      from,
      to,
      count: (res.messages || []).length,
      state: this.snapshot(st.symbol),
    });
    await this.drain(st);

    if (missing.length) {
      // Partial response: request the still-missing span again. A 410 there
      // is what flips the symbol to unrecoverable.
      const firstGap = missing[0];
      const lastGap = missing[missing.length - 1];
      if (firstGap > st.applied) {
        this.startBackfill(st, firstGap, lastGap);
        return;
      }
    }
    this.afterWindow(st);
  }

  onBackfillError(st, from, to, err) {
    if (!st.backfilling) return;
    st.backfilling = false;
    st.inflight = null;
    st.lastError = err && err.message ? err.message : String(err);

    const expired = err && (err.status === 410 || err.expired);
    if (expired) {
      // The server no longer stores the beginning of the requested range; the
      // permanently lost span is from our cursor up to its earliest retained seq.
      let gapFrom = from;
      let gapTo = to;
      if (Number.isInteger(err.earliestSeq)) {
        if (err.earliestSeq > from) {
          gapFrom = from;
          gapTo = Math.min(to, err.earliestSeq - 1);
        } else {
          gapFrom = from;
        }
      }
      return this.markUnrecoverable(st, gapFrom, gapTo, st.lastError, err);
    }
    // Transient failure: retry with capped backoff; live messages stay parked.
    this.emit(EVENT.BACKFILL_DONE, {
      symbol: st.symbol,
      from,
      to,
      error: st.lastError,
      retry: true,
      state: this.snapshot(st.symbol),
    });
    setTimeout(() => {
      if (!this.stopped && st.status === 'normal' && !st.backfilling) {
        this.startBackfill(st, from, to);
      }
    }, Math.min(5000, 500 * st.backfillAttempts));
  }

  markUnrecoverable(st, from, to, reason, raw) {
    st.status = 'unrecoverable';
    st.gap = { from, to, reason, at: this.now() };
    this.storage.putProgress(st.symbol, this.progressOf(st)).catch((err) => {
      st.lastError = String(err && err.message || err);
    });
    this.emit(EVENT.UNRECOVERABLE, {
      symbol: st.symbol,
      from,
      to,
      reason,
      raw: raw || null,
      state: this.snapshot(st.symbol),
    });
  }

  /**
   * Operator action: acknowledge that [gap.from..gap.to] is permanently lost,
   * advance the cursor to `to` and drain parked messages in order.
   */
  async skipGap(symbol) {
    const st = this.symbols.get(symbol);
    if (!st || st.status !== 'unrecoverable' || !st.gap) return false;
    const { to } = st.gap;
    st.applied = Math.max(st.applied, to);
    st.status = 'normal';
    st.gap = null;
    await this.storage.putProgress(symbol, this.progressOf(st));
    this.emit(EVENT.GAP_SKIPPED, { symbol, applied: st.applied, state: this.snapshot(symbol) });
    await this.drain(st);
    this.afterWindow(st);
    return true;
  }

  /**
   * Fault-helper: atomically rewind one symbol's durable state by dropping the
   * last `dropCount` applied records (and the matching progress), then
   * re-request exactly that span. With the server history deleted, the
   * backfill comes back 410 and the symbol flips to unrecoverable.
   * Records and progress move together inside one storage transaction.
   */
  async rewindSymbol(symbol, dropCount) {
    const st = this.symbols.get(symbol);
    if (!st || dropCount <= 0 || st.applied === 0) return false;
    const oldApplied = st.applied;
    const target = Math.max(0, oldApplied - dropCount);

    // clear transient routing state; parked messages will be re-discovered
    st.pending.clear();
    st.backfilling = false;
    st.inflight = null;
    st.backfillAttempts = 0;
    st.status = 'normal';
    st.gap = null;

    for (const seq of [...st.records.keys()]) if (seq > target) st.records.delete(seq);
    st.applied = target;
    st.latest = target > 0 ? st.records.get(target) || null : null;

    if (this.storage.rewind) {
      await this.storage.rewind(symbol, target, this.progressOf(st));
    } else {
      // adapters without rewind: best-effort equivalent
      for (let seq = target + 1; seq <= oldApplied; seq++) await this.storage.deleteMessage(symbol, seq);
      await this.storage.putProgress(symbol, this.progressOf(st));
    }

    this.emit(EVENT.GAP, {
      symbol,
      kind: 'rewind',
      from: target + 1,
      to: oldApplied,
      message: `${symbol} 本地状态回退至 #${target}，重新补查 #${target + 1}–#${oldApplied}`,
    });
    if (oldApplied > target) this.startBackfill(st, target + 1, oldApplied);
    return true;
  }

  afterWindow(st) {
    // If more parked messages remain, another gap may sit between cursor and
    // the first parked seq — backfill it.
    if (st.status !== 'normal' || st.backfilling || st.pending.size === 0) return;
    const firstParked = Math.min(...st.pending.keys());
    if (firstParked > st.applied + 1) {
      this.startBackfill(st, st.applied + 1, firstParked - 1);
    }
  }

  // -- conflicts -----------------------------------------------------------

  recordConflict(st, { seq, stored, incoming, source }) {
    const entry = {
      seq,
      at: this.now(),
      source,
      stored: this.publicRecord(stored),
      incoming: this.publicRecord(incoming),
    };
    st.conflicts.push(entry);
    if (st.conflicts.length > MAX_CONFLICTS) st.conflicts.shift();
    st.conflictCount += 1;
    this.storage
      .putProgress(st.symbol, this.progressOf(st))
      .then(() => this.emit(EVENT.CONFLICT, { symbol: st.symbol, conflict: entry, state: this.snapshot(st.symbol) }))
      .catch((err) => {
        st.lastError = String(err && err.message || err);
        this.emit(EVENT.CONFLICT, { symbol: st.symbol, conflict: entry, state: this.snapshot(st.symbol) });
      });
  }

  publicRecord(rec) {
    if (!rec) return null;
    const { injected, __source, ...rest } = rec;
    return { ...rest, injected };
  }

  // -- snapshots / progress ------------------------------------------------

  progressOf(st) {
    return {
      symbol: st.symbol,
      applied: st.applied,
      latest: st.latest,
      conflicts: st.conflicts,
      conflictCount: st.conflictCount,
      status: st.status,
      gap: st.gap,
      updatedAt: this.now(),
    };
  }

  snapshot(symbol) {
    const st = this.symbols.get(symbol);
    if (!st) return null;
    return {
      symbol,
      applied: st.applied,
      latest: st.latest,
      status: st.status,
      gap: st.gap,
      backfilling: st.backfilling,
      inflight: st.inflight,
      backfillAttempts: st.backfillAttempts,
      pendingCount: st.pending.size,
      pendingSeqs: [...st.pending.keys()].sort((a, b) => a - b),
      conflictCount: st.conflictCount,
      conflicts: [...st.conflicts].reverse(),
      lastError: st.lastError,
      recordCount: st.records.size,
    };
  }

  getRecords(symbol) {
    const st = this.symbols.get(symbol);
    return st ? [...st.records.values()].sort((a, b) => a.seq - b.seq) : [];
  }
}

// Backfill helper shared by browser and Node tests.
export function httpBackfill(base) {
  return async (symbol, from, to) => {
    const url = `${base}/api/backfill?symbol=${encodeURIComponent(symbol)}&from=${from}&to=${to}`;
    const res = await fetch(url);
    let body = null;
    try {
      body = await res.json();
    } catch {
      /* non-json error */
    }
    if (!res.ok) {
      const err = new Error((body && body.error) || `backfill HTTP ${res.status}`);
      err.status = res.status;
      if (body) Object.assign(err, body);
      throw err;
    }
    return body;
  };
}
