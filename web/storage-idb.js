// IndexedDB persistence adapter.
//
// Durability contract:
//   put(symbol, message, progress) writes the new message and the advanced
//   progress cursor in ONE IndexedDB transaction. The engine only updates its
//   in-memory cursor after the transaction resolves, so a crash at any point
//   leaves both at the old value — progress can never run ahead of records.

const DB_NAME = 'quote-receiver-v1';
const DB_VERSION = 1;
const STORE_MESSAGES = 'messages'; // keyPath [symbol, seq]
const STORE_PROGRESS = 'progress'; // keyPath symbol

function openDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE_MESSAGES)) {
        const store = db.createObjectStore(STORE_MESSAGES, { keyPath: ['symbol', 'seq'] });
        store.createIndex('symbol', 'symbol', { unique: false });
      }
      if (!db.objectStoreNames.contains(STORE_PROGRESS)) {
        db.createObjectStore(STORE_PROGRESS, { keyPath: 'symbol' });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function txDone(tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error || new Error('transaction aborted'));
  });
}

export class IndexedStorage {
  constructor() {
    this.db = null;
  }

  async init() {
    if (!this.db) this.db = await openDb();
  }

  /** Atomic message + progress write. */
  async put(symbol, message, progress) {
    await this.init();
    const tx = this.db.transaction([STORE_MESSAGES, STORE_PROGRESS], 'readwrite');
    tx.objectStore(STORE_MESSAGES).put(message);
    tx.objectStore(STORE_PROGRESS).put(progress);
    await txDone(tx);
  }

  /** Progress-only write (conflict annotations, gap state changes). */
  async putProgress(symbol, progress) {
    await this.init();
    const tx = this.db.transaction([STORE_PROGRESS], 'readwrite');
    tx.objectStore(STORE_PROGRESS).put(progress);
    await txDone(tx);
  }

  async getMessages(symbol) {
    await this.init();
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(STORE_MESSAGES, 'readonly');
      const req = tx.objectStore(STORE_MESSAGES).index('symbol').getAll(symbol);
      req.onsuccess = () => resolve(req.result || []);
      req.onerror = () => reject(req.error);
    });
  }

  async getAllProgress() {
    await this.init();
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(STORE_PROGRESS, 'readonly');
      const req = tx.objectStore(STORE_PROGRESS).getAll();
      req.onsuccess = () => {
        const out = {};
        for (const p of req.result || []) out[p.symbol] = p;
        resolve(out);
      };
      req.onerror = () => reject(req.error);
    });
  }

  async deleteMessage(symbol, seq) {
    await this.init();
    const tx = this.db.transaction(STORE_MESSAGES, 'readwrite');
    tx.objectStore(STORE_MESSAGES).delete([symbol, seq]);
    await txDone(tx);
  }

  /**
   * Atomic rollback: delete every message with seq > keepApplied for the
   * symbol and write the rewound progress in ONE transaction. Used by the
   * "delete history" fault to force the engine to re-request a span that the
   * server has already expired. Either both changes land or neither does.
   */
  async rewind(symbol, keepApplied, progress) {
    await this.init();
    const tx = this.db.transaction([STORE_MESSAGES, STORE_PROGRESS], 'readwrite');
    const msgStore = tx.objectStore(STORE_MESSAGES);
    const index = msgStore.index('symbol');
    const req = index.openCursor(symbol);
    req.onsuccess = () => {
      const cursor = req.result;
      if (!cursor) return;
      if (cursor.value.seq > keepApplied) cursor.delete();
      cursor.continue();
    };
    tx.objectStore(STORE_PROGRESS).put(progress);
    await txDone(tx);
  }

  async clear() {
    await this.init();
    const tx = this.db.transaction([STORE_MESSAGES, STORE_PROGRESS], 'readwrite');
    tx.objectStore(STORE_MESSAGES).clear();
    tx.objectStore(STORE_PROGRESS).clear();
    await txDone(tx);
  }
}
