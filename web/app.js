import { QuoteEngine, EVENT } from './engine.js';
import { IndexedStorage } from './storage-idb.js';

const $ = (id) => document.getElementById(id);

const SOURCE_LABEL = { live: '实时', backfill: '补查', anchor: '锚定' };
const CONN_LABEL = {
  stopped: '已停止',
  connecting: '连接中…',
  online: '在线',
  reconnecting: '重连中…',
  degraded: '心跳超时',
};

const storage = new IndexedStorage();
const engine = new QuoteEngine({
  storage,
  transport: () => new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`),
  backfill: (symbol, from, to) =>
    fetch(`/api/backfill?symbol=${encodeURIComponent(symbol)}&from=${from}&to=${to}`).then(async (res) => {
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        const err = new Error(body.error || `HTTP ${res.status}`);
        Object.assign(err, body, { status: res.status });
        throw err;
      }
      return body;
    }),
});

let selected = null;
let blackholeOn = false;
const cardFlash = new Map();

// ---------------------------------------------------------------------------
// Engine events
// ---------------------------------------------------------------------------

engine.addEventListener(EVENT.CONNECTION, (e) => {
  const { status } = e.detail;
  const badge = $('connBadge');
  badge.className = `conn ${status}`;
  $('connText').textContent = CONN_LABEL[status] || status;
});

engine.addEventListener(EVENT.SYMBOL, () => renderCards());
engine.addEventListener(EVENT.APPLIED, (e) => {
  const { symbol } = e.detail;
  cardFlash.set(symbol, Date.now());
  renderCards();
  if (selected === symbol) renderDetail();
});
engine.addEventListener(EVENT.GAP, (e) => toast(e.detail.message || `检测到 ${e.detail.symbol} 缺口`));
engine.addEventListener(EVENT.BACKFILL_START, (e) => {
  toast(`${e.detail.symbol} 检测缺口 #${e.detail.from}–#${e.detail.to}，发起补查…`);
  renderCards();
  if (selected === e.detail.symbol) renderDetail();
});
engine.addEventListener(EVENT.BACKFILL_DONE, (e) => {
  if (e.detail.error) toast(`${e.detail.symbol} 补查失败：${e.detail.error}（将重试）`, true);
  renderCards();
  if (selected === e.detail.symbol) renderDetail();
});
engine.addEventListener(EVENT.UNRECOVERABLE, (e) => {
  toast(`${e.detail.symbol} 无法恢复：#${e.detail.from}–#${e.detail.to} 记录已过期`, true);
  renderCards();
  if (selected === e.detail.symbol) renderDetail();
});
engine.addEventListener(EVENT.GAP_SKIPPED, () => {
  renderCards();
  renderDetail();
});
engine.addEventListener(EVENT.CONFLICT, (e) => {
  toast(`${e.detail.symbol} 序号 #${e.detail.conflict.seq} 内容冲突，已标记未自动取舍`, true);
  renderCards();
  if (selected === e.detail.symbol) renderDetail();
});

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

function renderCards() {
  const wrap = $('cards');
  const symbols = [...engine.symbols.keys()].sort();
  if (!symbols.length) {
    wrap.innerHTML = `<p class="sub" style="padding:8px 4px">启动后等待行情推送…</p>`;
    return;
  }
  wrap.innerHTML = '';
  for (const symbol of symbols) {
    const s = engine.snapshot(symbol);
    const card = document.createElement('div');
    card.className = `card${selected === symbol ? ' selected' : ''}`;
    const flash = cardFlash.has(symbol);
    const badges = [];
    if (s.status === 'unrecoverable') badges.push(`<span class="badge dead">无法恢复</span>`);
    if (s.backfilling) badges.push(`<span class="badge backfill">补查中 #${s.inflight.from}–#${s.inflight.to}</span>`);
    if (s.pendingCount) badges.push(`<span class="badge pending">暂存 ${s.pendingCount}</span>`);
    if (s.conflictCount) badges.push(`<span class="badge conflict">冲突 ${s.conflictCount}</span>`);
    badges.push(`<span class="badge live">序号 #${s.applied}</span>`);

    card.innerHTML = `
      <div class="card-top">
        <span class="card-symbol">${symbol}</span>
        <span class="card-seq">最新 #${s.applied} · 落盘 ${s.recordCount}</span>
      </div>
      <div class="card-price ${flash ? 'flash' : ''}">${s.latest ? s.latest.price.toLocaleString() : '—'}</div>
      <div class="card-row">
        <span>买 ${s.latest ? s.latest.bid : '–'} / 卖 ${s.latest ? s.latest.ask : '–'}</span>
        <span>${s.backfilling ? '补齐中' : s.status === 'unrecoverable' ? '中断' : '接收中'}</span>
      </div>
      <div class="card-badges">${badges.join('')}</div>
    `;
    card.onclick = () => selectSymbol(symbol);
    wrap.appendChild(card);
  }
}

function selectSymbol(symbol) {
  selected = symbol;
  renderCards();
  renderDetail();
}

function renderDetail() {
  if (!selected) return;
  const s = engine.snapshot(selected);
  if (!s) return;
  $('detailEmpty').hidden = true;
  $('detailBody').hidden = false;

  $('dSymbol').textContent = selected;
  $('dMeta').textContent =
    `进度序号 #${s.applied} · 暂存 ${s.pendingCount} 条 · 补查 ${s.backfillAttempts} 次 · 状态 ${
      s.status === 'unrecoverable' ? '无法恢复' : s.backfilling ? '补查中' : '正常'
    }`;

  // banner
  const banner = $('banner');
  banner.hidden = false;
  if (s.status === 'unrecoverable' && s.gap) {
    banner.className = 'banner dead';
    banner.innerHTML =
      `<b>无法恢复</b>：序号 #${s.gap.from}–#${s.gap.to} 的补查记录已过期（${s.gap.reason}）。` +
      `后续消息已暂存（${s.pendingCount} 条），最新报价停在 #${s.applied}，不会被旧/乱序消息覆盖。` +
      `其他交易对不受影响，仍正常接收。`;
    $('btnSkip').hidden = false;
  } else if (s.backfilling) {
    banner.className = 'banner warn';
    banner.textContent = `发现缺口 #${s.inflight.from}–#${s.inflight.to}：后续消息已暂存，等待 HTTP 补查按序补齐…`;
    $('btnSkip').hidden = true;
  } else if (s.pendingCount) {
    banner.className = 'banner info';
    banner.textContent = `有 ${s.pendingCount} 条消息暂存（序号 ${s.pendingSeqs.slice(0, 8).join(', ')}${
      s.pendingSeqs.length > 8 ? '…' : ''
    }），等待缺口补齐后按序生效。`;
    $('btnSkip').hidden = true;
  } else {
    banner.className = 'banner info';
    banner.textContent = '序号连续，实时消息直接生效；重复消息只去重不覆盖，乱序消息先暂存。';
    $('btnSkip').hidden = true;
  }

  $('sApplied').textContent = `#${s.applied}`;
  $('sPending').textContent = s.pendingCount;
  $('sBackfill').textContent = s.backfillAttempts;
  $('sConflict').textContent = s.conflictCount;
  $('sStored').textContent = s.recordCount;

  const latest = s.latest;
  $('lPrice').textContent = latest ? latest.price.toLocaleString() : '—';
  $('lReason').textContent = latest ? latest.reason : '';
  $('lBid').textContent = latest ? latest.bid : '–';
  $('lAsk').textContent = latest ? latest.ask : '–';
  $('lVol').textContent = latest ? latest.volume : '–';
  $('lSource').textContent = latest ? `${SOURCE_LABEL[latest.source] || latest.source} · #${latest.seq}` : '–';

  // conflicts
  const cbox = $('conflictBox');
  if (s.conflicts.length) {
    cbox.hidden = false;
    $('conflictList').innerHTML = s.conflicts
      .map(
        (c) => `
      <div class="conflict-item">
        <b>序号 #${c.seq} 冲突</b>（${c.source}，${new Date(c.at).toLocaleTimeString()}）
        <div class="row">
          <span>已落盘内容：<code>${escapeHtml(JSON.stringify(c.stored))}</code></span>
        </div>
        <div class="row">
          <span>后到内容：<code>${escapeHtml(JSON.stringify(c.incoming))}</code></span>
        </div>
        <div class="row"><span class="sub">系统未自动取舍：最新报价保留已落盘版本，冲突留痕。</span></div>
      </div>`,
      )
      .join('');
  } else {
    cbox.hidden = true;
  }

  // message list (newest first)
  const records = engine.getRecords(selected);
  $('msgHint').textContent = `共 ${records.length} 条（最新在上）`;
  $('msgList').innerHTML = records
    .slice()
    .reverse()
    .map((r, idx) => `
      <div class="msg ${idx === 0 ? 'new' : ''}">
        <span class="seq">#${r.seq}</span>
        <span class="src ${r.source}">${SOURCE_LABEL[r.source] || r.source}</span>
        <span class="body">
          <div>时间 ${new Date(r.ts).toLocaleTimeString()} · 量 ${r.volume}</div>
          <div class="reason">${escapeHtml(r.reason)}</div>
        </span>
        <span class="price">${r.price.toLocaleString()}</span>
      </div>`)
    .join('');
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// ---------------------------------------------------------------------------
// Controls
// ---------------------------------------------------------------------------

$('btnStart').onclick = async () => {
  $('btnStart').disabled = true;
  await engine.start();
  $('btnStop').disabled = false;
  toast('接收已启动，进度从本地落盘恢复');
};
$('btnStop').onclick = () => {
  engine.stop();
  $('btnStart').disabled = false;
  $('btnStop').disabled = true;
  toast('接收已关闭，消息与进度已共同落盘');
};
$('btnClear').onclick = async () => {
  if (!confirm('确认清空全部本地落盘消息与进度？')) return;
  engine.stop();
  await storage.clear();
  engine.symbols.clear();
  selected = null;
  $('detailEmpty').hidden = false;
  $('detailBody').hidden = true;
  $('btnStart').disabled = false;
  $('btnStop').disabled = true;
  renderCards();
  toast('本地落盘已清空');
};

async function admin(action, body) {
  const res = await fetch(`/api/admin/${action}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body || {}),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) toast(`操作失败：${data.error || res.status}`, true);
  return data;
}

$('btnDisconnect').onclick = () => admin('disconnect').then(() => toast('已强制断开 WebSocket，客户端将自动重连'));
$('btnBlackhole').onclick = async () => {
  blackholeOn = !blackholeOn;
  await admin('network', { mode: blackholeOn ? 'off' : 'on' });
  $('bhState').textContent = blackholeOn ? '开' : '关';
  toast(blackholeOn ? '已模拟断网：推送静默，重连后按序号补查缺口' : '网络已恢复');
};

function requireSymbol() {
  if (!selected) {
    toast('请先在左侧选择一个交易对', true);
    return null;
  }
  return selected;
}
$('btnDup').onclick = () => {
  const sym = requireSymbol();
  if (sym) admin('duplicate', { symbol: sym, times: 2 }).then((d) => toast(`${sym} 已重发序号 #${d.seq} ×${d.times}（客户端应去重）`));
};
$('btnReorder').onclick = () => {
  const sym = requireSymbol();
  if (sym) admin('reorder', { symbol: sym }).then((d) => toast(`${sym} 已插入旧序号 #${d.seq}（不得覆盖最新报价）`));
};
$('btnConflict').onclick = () => {
  const sym = requireSymbol();
  if (sym) admin('conflict', { symbol: sym }).then((d) => toast(`${sym} 已注入 #${d.seq} 冲突内容，注意冲突标记`));
};
$('btnDeleteGap').onclick = async () => {
  const sym = requireSymbol();
  if (!sym) return;
  const s = engine.snapshot(sym);
  const to = s.applied;
  const from = Math.max(1, to - 2);
  // 1) server forgets the span, so any backfill for it returns 410
  await admin('delete', { symbol: sym, from, to });
  // 2) atomically rewind local state by the same span and re-request it;
  //    records + progress roll back in one transaction, then backfill fails expired
  await engine.rewindSymbol(sym, to - from + 1);
  toast(`${sym} 已删除服务端历史并原子回退本地 #${from}–#${to}，补查将判定无法恢复`, true);
};
$('btnSkip').onclick = () => {
  if (selected && engine.skipGap(selected)) toast('已承认缺口并跳过，暂存消息开始按序生效');
};

// toast
let toastTimer = null;
function toast(text, isErr = false) {
  const el = $('toast');
  el.textContent = text;
  el.className = isErr ? 'toast err' : 'toast';
  el.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (el.hidden = true), 3600);
}

renderCards();
