// In-memory implementation of the storage adapter contract used by the engine.
// Mirrors IndexedStorage semantics: put() is atomic (all-or-nothing per call).

export class MemoryStorage {
  constructor() {
    this.messages = new Map(); // symbol -> Map(seq -> rec)
    this.progress = new Map(); // symbol -> progress
    this.failOnce = null; // 'put' | 'progress' to simulate a failed transaction
  }

  async init() {
    /* noop */
  }

  async put(symbol, message, progress) {
    if (this.failOnce === 'put') {
      this.failOnce = null;
      throw new Error('simulated transaction failure');
    }
    if (!this.messages.has(symbol)) this.messages.set(symbol, new Map());
    // Atomic-ish: build new state first, commit at the end.
    const next = new Map(this.messages.get(symbol));
    next.set(message.seq, message);
    this.messages.set(symbol, next);
    this.progress.set(symbol, structuredClone(progress));
  }

  async putProgress(symbol, progress) {
    if (this.failOnce === 'progress') {
      this.failOnce = null;
      throw new Error('simulated progress failure');
    }
    this.progress.set(symbol, structuredClone(progress));
  }

  async getMessages(symbol) {
    return [...(this.messages.get(symbol)?.values() || [])].map((r) => structuredClone(r));
  }

  async getAllProgress() {
    return Object.fromEntries([...this.progress.entries()].map(([k, v]) => [k, structuredClone(v)]));
  }

  async deleteMessage(symbol, seq) {
    this.messages.get(symbol)?.delete(seq);
  }

  async rewind(symbol, keepApplied, progress) {
    const map = this.messages.get(symbol);
    if (map) for (const seq of [...map.keys()]) if (seq > keepApplied) map.delete(seq);
    this.progress.set(symbol, structuredClone(progress));
  }

  async clear() {
    this.messages.clear();
    this.progress.clear();
  }
}

// Fake socket with the subset of the WebSocket surface the engine relies on.
export class FakeSocket {
  constructor() {
    this.readyState = 0;
    this.sent = [];
    this.onopen = null;
    this.onmessage = null;
    this.onclose = null;
    this.onerror = null;
  }
  send(text) {
    this.sent.push(JSON.parse(text));
  }
  open() {
    this.readyState = 1;
    this.onopen?.({});
  }
  deliver(obj) {
    this.onmessage?.({ data: JSON.stringify(obj) });
  }
  close() {
    this.readyState = 3;
    this.onclose?.({});
  }
}

export function quote(symbol, seq, overrides = {}) {
  return {
    type: 'quote',
    symbol,
    seq,
    ts: 1_700_000_000_000 + seq * 1000,
    price: 100 + seq,
    bid: 99.9 + seq,
    ask: 100.1 + seq,
    volume: 1.5,
    ...overrides,
  };
}

export function waitFor(fn, { timeout = 2000, interval = 10, label = 'condition' } = {}) {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const tick = () => {
      try {
        if (fn()) return resolve();
      } catch (e) {
        return reject(e);
      }
      if (Date.now() - start > timeout) return reject(new Error(`timeout waiting for ${label}`));
      setTimeout(tick, interval);
    };
    tick();
  });
}
