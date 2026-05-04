"""Browser UI HTML — served at GET /browser.

Call make_browser_html(tree, regions) with the results from sde_market to
get a bytes HTML page.  The market-group tree and region list are embedded
directly in the page as JS constants, so there are zero API round-trips on
initial load — the tree appears instantly.  The only network calls made by
the page are when the user actually selects an item.
"""
import json

# ---------------------------------------------------------------------------
# Template — two placeholders replaced by make_browser_html()
# ---------------------------------------------------------------------------

_TMPL = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>EVE Market</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/uplot@1.6.31/dist/uPlot.min.css">
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0 }
:root {
  --bg:  #0b0d14; --bg2: #111420; --bg3: #161b2e; --bg4: #1b2138;
  --bd:  #252d45; --bd2: #2e3858;
  --tx:  #8a9bb8; --tx2: #56687a; --hd:  #c5d6ee;
  --ac:  #3e7cb5; --ac2: #5592cc;
  --sl:  #c04040; --sl2: #e06060;
  --by:  #358a46; --by2: #48c060;
}
html, body { height: 100%; overflow: hidden; background: var(--bg); color: var(--tx);
  font: 13px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif }

/* ── Layout ── */
#wrap  { display: flex; height: 100vh }
#side  { width: 270px; flex-shrink: 0; display: flex; flex-direction: column;
         background: var(--bg2); border-right: 1px solid var(--bd); overflow: hidden }
#main  { flex: 1; display: flex; flex-direction: column; overflow: hidden; min-width: 0 }

/* ── Search ── */
#sq    { padding: 7px; border-bottom: 1px solid var(--bd); flex-shrink: 0; position: relative }
#si    { width: 100%; background: var(--bg3); border: 1px solid var(--bd2); border-radius: 4px;
         padding: 5px 8px 5px 26px; color: var(--hd); font-size: 13px; outline: none }
#si::placeholder { color: var(--tx2) }
#si:focus { border-color: var(--ac2) }
#si-ic { position: absolute; left: 15px; top: 50%; transform: translateY(-50%);
         color: var(--tx2); font-size: 11px; pointer-events: none }
#sr    { display: none; position: absolute; left: 7px; right: 7px; top: 100%; z-index: 50;
         background: var(--bg3); border: 1px solid var(--bd2);
         border-radius: 0 0 4px 4px; max-height: 320px; overflow-y: auto }
#sr.on { display: block }
.ri    { padding: 5px 10px; cursor: pointer; border-bottom: 1px solid var(--bd) }
.ri:last-child { border-bottom: none }
.ri:hover, .ri.foc { background: var(--bg4) }
.ri-n  { color: var(--hd); font-size: 13px }
.ri-g  { color: var(--tx2); font-size: 11px }
.r-msg { padding: 10px; text-align: center; color: var(--tx2); font-size: 12px }

/* ── Nav (tree / type-list) ── */
#nav   { flex: 1; overflow: hidden; display: flex; flex-direction: column }
.view  { display: none; flex: 1; flex-direction: column; overflow: hidden }
.view.on { display: flex }
#tv    { overflow-y: auto }

/* Tree nodes */
.tn-row  { display: flex; align-items: center; padding: 4px 6px; cursor: pointer;
           border-bottom: 1px solid var(--bd); white-space: nowrap; overflow: hidden }
.tn-row:hover { background: var(--bg3) }
.tn-caret { width: 14px; font-size: 10px; color: var(--tx2); flex-shrink: 0;
            text-align: center; transition: transform .12s }
.tn-caret.op { transform: rotate(90deg) }
.tn-lbl  { overflow: hidden; text-overflow: ellipsis; font-size: 13px; flex: 1 }
.tn-kids { display: none }
.tn-kids.op { display: block }

/* Type list */
#tl-hdr  { display: flex; align-items: center; gap: 6px; padding: 6px 8px;
           background: var(--bg3); border-bottom: 1px solid var(--bd); flex-shrink: 0 }
#tl-back { background: none; border: none; color: var(--ac2); cursor: pointer;
           font-size: 18px; padding: 0 2px; line-height: 1 }
#tl-back:hover { color: var(--hd) }
#tl-name { font-size: 12px; font-weight: 600; color: var(--hd);
           overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1 }
#tl-body { flex: 1; overflow-y: auto }
.trow    { padding: 5px 12px; cursor: pointer; border-bottom: 1px solid var(--bd);
           font-size: 13px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis }
.trow:hover { background: var(--bg4); color: var(--hd) }
.trow.sel   { color: var(--ac2); background: var(--bg4) }

/* ── Empty / Item panels ── */
#mp-empty { flex: 1; display: flex; align-items: center; justify-content: center;
            flex-direction: column; gap: 8px; color: var(--tx2) }
#mp-empty b     { font-size: 15px; color: var(--hd) }
#mp-empty small { font-size: 11px }
#mp-item  { flex: 1; display: none; flex-direction: column; overflow: hidden }
#mp-item.on { display: flex }

/* Item header */
#ih    { display: flex; align-items: center; gap: 10px; padding: 9px 12px;
         background: var(--bg2); border-bottom: 1px solid var(--bd); flex-shrink: 0 }
#ii    { width: 48px; height: 48px; border-radius: 3px; background: var(--bg3);
         flex-shrink: 0; object-fit: cover }
#im    { flex: 1; min-width: 0 }
#ipath { font-size: 11px; color: var(--tx2); white-space: nowrap;
         overflow: hidden; text-overflow: ellipsis }
#iname { font-size: 17px; font-weight: 600; color: var(--hd); white-space: nowrap;
         overflow: hidden; text-overflow: ellipsis }
#isc-w  { flex-shrink: 0; display: flex; align-items: center; gap: 6px }
#isc-lb { font-size: 11px; color: var(--tx2) }
#isc    { background: var(--bg3); border: 1px solid var(--bd2); border-radius: 4px;
          padding: 4px 7px; color: var(--hd); font-size: 12px; outline: none; cursor: pointer }
#isc:focus { border-color: var(--ac2) }

/* Stats bar */
#sb     { display: flex; border-bottom: 1px solid var(--bd); flex-shrink: 0; background: var(--bg2) }
.st     { flex: 1; padding: 5px 10px; border-right: 1px solid var(--bd); min-width: 0 }
.st:last-child { border-right: none }
.st-l   { font-size: 10px; text-transform: uppercase; letter-spacing: .06em;
          color: var(--tx2); white-space: nowrap }
.st-v   { font-size: 13px; font-weight: 600; color: var(--hd);
          white-space: nowrap; overflow: hidden; text-overflow: ellipsis }
.st-v.s { color: var(--sl2) }
.st-v.b { color: var(--by2) }

/* Item body */
#ib     { flex: 1; overflow-y: auto; padding: 10px; display: flex;
          flex-direction: column; gap: 10px }
.bx     { background: var(--bg2); border: 1px solid var(--bd); border-radius: 5px; overflow: hidden }
.bx-h   { display: flex; align-items: center; justify-content: space-between;
          padding: 6px 10px; background: var(--bg3); border-bottom: 1px solid var(--bd) }
.bx-t   { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: .06em }
.bx-t.s { color: var(--sl2) }
.bx-t.b { color: var(--by2) }
.bx-t.n { color: var(--tx2) }
.bx-m   { font-size: 11px; color: var(--tx2) }
#ch-el  { padding: 8px 4px }
.uplot  { background: transparent !important }

/* Tables */
.tw     { overflow-x: auto }
table   { width: 100%; border-collapse: collapse }
thead th { padding: 5px 8px; font-size: 10px; text-transform: uppercase;
           letter-spacing: .05em; color: var(--tx2); border-bottom: 1px solid var(--bd);
           text-align: left; cursor: pointer; white-space: nowrap; user-select: none;
           background: var(--bg3); position: sticky; top: 0 }
thead th:hover { color: var(--hd) }
thead th.asc::after  { content: " \2191"; color: var(--ac2) }
thead th.desc::after { content: " \2193"; color: var(--ac2) }
tbody td { padding: 4px 8px; border-bottom: 1px solid var(--bd); white-space: nowrap; font-size: 12px }
tbody tr:last-child td { border-bottom: none }
tbody tr:hover td  { background: var(--bg4) }
.ps  { color: var(--sl2); font-weight: 600 }
.pb  { color: var(--by2); font-weight: 600 }
.loc { color: var(--hd) }
.rgn { color: var(--tx2); font-size: 11px }
.h5  { color: #4ec44e } .h4 { color: #8bca4e } .h3 { color: #c8c840 }
.h2  { color: #d48030 } .h1 { color: #c84020 } .h0 { color: #b01818 }
.msg { padding: 12px; text-align: center; color: var(--tx2); font-size: 12px }
.sp  { display: inline-block; width: 11px; height: 11px; border: 2px solid var(--bd2);
       border-top-color: var(--ac2); border-radius: 50%;
       animation: spin .6s linear infinite; vertical-align: middle; margin-right: 4px }
@keyframes spin { to { transform: rotate(360deg) } }
::-webkit-scrollbar       { width: 5px; height: 5px }
::-webkit-scrollbar-track { background: var(--bg2) }
::-webkit-scrollbar-thumb { background: var(--bd2); border-radius: 3px }
</style>
</head>
<body>
<div id="wrap">
<aside id="side">
  <div id="sq">
    <span id="si-ic">&#9906;</span>
    <input id="si" type="search" placeholder="Search all items..." autocomplete="off">
    <div id="sr"></div>
  </div>
  <div id="nav">
    <div id="tv" class="view on"></div>
    <div id="tlv" class="view">
      <div id="tl-hdr">
        <button id="tl-back">&#8592;</button>
        <span id="tl-name"></span>
      </div>
      <div id="tl-body"></div>
    </div>
  </div>
</aside>
<main id="main">
  <div id="mp-empty">
    <b>EVE Market Browser</b>
    <small>Browse the tree on the left or search for an item</small>
  </div>
  <div id="mp-item">
    <div id="ih">
      <img id="ii" src="" alt="">
      <div id="im">
        <div id="ipath"></div>
        <div id="iname"></div>
      </div>
      <div id="isc-w">
        <span id="isc-lb">Region</span>
        <select id="isc"><option value="all">All Regions</option></select>
      </div>
    </div>
    <div id="sb">
      <div class="st"><div class="st-l">Best Sell</div><div id="st-sl" class="st-v s">&mdash;</div></div>
      <div class="st"><div class="st-l">Best Buy</div> <div id="st-by" class="st-v b">&mdash;</div></div>
      <div class="st"><div class="st-l">Margin</div>   <div id="st-mg" class="st-v">&mdash;</div></div>
      <div class="st"><div class="st-l">Sell Vol</div> <div id="st-sv" class="st-v">&mdash;</div></div>
      <div class="st"><div class="st-l">Buy Vol</div>  <div id="st-bv" class="st-v">&mdash;</div></div>
      <div class="st"><div class="st-l">Orders</div>   <div id="st-or" class="st-v">&mdash;</div></div>
    </div>
    <div id="ib">
      <div class="bx">
        <div class="bx-h">
          <span class="bx-t n">Price History</span>
          <span class="bx-m" id="ch-meta"></span>
        </div>
        <div id="ch-el"><div class="msg">Select an item to view history.</div></div>
      </div>
      <div class="bx">
        <div class="bx-h">
          <span class="bx-t s">&#9660; Sell Orders</span>
          <span class="bx-m" id="sl-meta"></span>
        </div>
        <div class="tw">
          <table>
            <thead><tr id="sl-hdr">
              <th data-c="price" data-t="s">Price</th>
              <th data-c="volume_remain" data-t="s">Qty</th>
              <th data-c="loc" data-t="s">Location</th>
              <th data-c="sec" data-t="s">Sec</th>
              <th data-c="exp" data-t="s">Expires</th>
            </tr></thead>
            <tbody id="sl-body"><tr><td colspan="5" class="msg">Select an item.</td></tr></tbody>
          </table>
        </div>
      </div>
      <div class="bx">
        <div class="bx-h">
          <span class="bx-t b">&#9650; Buy Orders</span>
          <span class="bx-m" id="by-meta"></span>
        </div>
        <div class="tw">
          <table>
            <thead><tr id="by-hdr">
              <th data-c="price" data-t="b">Price</th>
              <th data-c="volume_remain" data-t="b">Qty</th>
              <th data-c="range" data-t="b">Range</th>
              <th data-c="min_volume" data-t="b">Min Vol</th>
              <th data-c="loc" data-t="b">Location</th>
              <th data-c="sec" data-t="b">Sec</th>
              <th data-c="exp" data-t="b">Expires</th>
            </tr></thead>
            <tbody id="by-body"><tr><td colspan="7" class="msg">Select an item.</td></tr></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>
</main>
</div>
<script src="https://cdn.jsdelivr.net/npm/uplot@1.6.31/dist/uPlot.iife.min.js"></script>
<script>
/* ── Embedded SDE data (no API round-trips needed) ── */
const TREE    = /*TREE*/;
const REGIONS = /*REGIONS*/;

/* ── Helpers ── */
const $ = id => document.getElementById(id);
const esc = s => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

function fmtISK(v) {
  if (v == null || isNaN(v)) return '\u2014';
  if (v >= 1e12) return (v/1e12).toFixed(2)+' T';
  if (v >= 1e9)  return (v/1e9).toFixed(2)+' B';
  if (v >= 1e6)  return (v/1e6).toFixed(2)+' M';
  if (v >= 1e3)  return (v/1e3).toFixed(1)+' K';
  return v.toLocaleString(undefined, {maximumFractionDigits: 2});
}
function fmtN(v) { return v == null ? '\u2014' : Number(v).toLocaleString(); }
function secCls(s) {
  const v = s == null ? -1 : Math.max(0, s);
  if (v >= 0.9) return 'h5'; if (v >= 0.5) return 'h4';
  if (v >= 0.3) return 'h3'; if (v >= 0.1) return 'h2';
  if (v >  0  ) return 'h1'; return 'h0';
}
function fmtSec(s) { return s == null ? '?' : Math.max(0, s).toFixed(1); }
function fmtExp(issued, dur) {
  if (!issued) return '\u2014';
  const ms = new Date(issued).getTime() + (dur || 0)*86400000 - Date.now();
  if (ms < 0) return 'expired';
  const d = Math.floor(ms / 86400000);
  if (d >= 1) return d + 'd';
  return Math.floor(ms / 3600000) + 'h';
}
function pct(sorted, p) {
  if (!sorted.length) return null;
  return sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)];
}

/* ── State ── */
const S = {
  tid: null,
  orders: [],
  names: {},
  chart: null,
  sort: { s: {c:'price',d:'asc'}, b: {c:'price',d:'desc'} },
};

/* ── Populate region selector from inlined data ── */
(function() {
  const sel = $('isc');
  for (const r of REGIONS) {
    const o = document.createElement('option');
    o.value = String(r.id); o.textContent = r.name;
    sel.appendChild(o);
  }
})();

$('isc').addEventListener('change', () => {
  if (!S.tid) return;
  computeStats(); renderTables(); loadHistory(S.tid);
});
function regionId() { const v = $('isc').value; return v === 'all' ? null : +v; }

/* ── Search ── */
let sdebounce = null, sfocus = -1;
$('si').addEventListener('input', e => {
  clearTimeout(sdebounce);
  const q = e.target.value.trim();
  if (!q) { hideSearch(); return; }
  sdebounce = setTimeout(() => doSearch(q), 220);
});
$('si').addEventListener('keydown', e => {
  const items = $('sr').querySelectorAll('.ri');
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    sfocus = Math.min(sfocus+1, items.length-1);
    items.forEach((el,i) => el.classList.toggle('foc', i===sfocus));
    if (items[sfocus]) items[sfocus].scrollIntoView({block:'nearest'});
  } else if (e.key === 'ArrowUp') {
    e.preventDefault();
    sfocus = Math.max(sfocus-1, 0);
    items.forEach((el,i) => el.classList.toggle('foc', i===sfocus));
    if (items[sfocus]) items[sfocus].scrollIntoView({block:'nearest'});
  } else if (e.key === 'Enter') {
    const t = items[sfocus >= 0 ? sfocus : 0];
    if (t) t.dispatchEvent(new MouseEvent('mousedown'));
  } else if (e.key === 'Escape') { hideSearch(); }
});
$('si').addEventListener('blur', () => setTimeout(hideSearch, 150));
function hideSearch() { $('sr').classList.remove('on'); sfocus = -1; }

async function doSearch(q) {
  const sr = $('sr');
  sr.innerHTML = '<div class="r-msg"><span class="sp"></span></div>';
  sr.classList.add('on'); sfocus = -1;
  try {
    const res = await fetch('/api/search_types?q=' + encodeURIComponent(q)).then(r => r.json());
    if (!res.length) { sr.innerHTML = '<div class="r-msg">No results</div>'; return; }
    sr.innerHTML = '';
    for (const m of res) {
      const d = document.createElement('div');
      d.className = 'ri';
      d.innerHTML = '<div class="ri-n">'+esc(m.name)+'</div><div class="ri-g">'+esc(m.groupName)+'</div>';
      d.addEventListener('mousedown', () => { $('si').value = ''; hideSearch(); selectType(m.id, m.name); });
      sr.appendChild(d);
    }
    if (res.length === 100) {
      const more = document.createElement('div');
      more.className = 'r-msg'; more.textContent = 'Showing top 100 \u2014 refine your search';
      sr.appendChild(more);
    }
  } catch(e) { sr.innerHTML = '<div class="r-msg">Search failed</div>'; }
}

/* ── Tree (rendered from inlined TREE constant) ── */
(function buildTree() {
  const tv = $('tv');
  for (const node of TREE) tv.appendChild(mkNode(node, 0));
})();

function mkNode(node, depth) {
  const wrap = document.createElement('div');
  const row  = document.createElement('div');
  row.className = 'tn-row';
  row.style.paddingLeft = (6 + depth*14) + 'px';

  const hasKids = node.children && node.children.length > 0;
  const car = document.createElement('span');
  car.className = 'tn-caret';
  car.textContent = hasKids ? '\u25BA' : ' ';

  const lbl = document.createElement('span');
  lbl.className = 'tn-lbl'; lbl.textContent = node.name;

  row.appendChild(car); row.appendChild(lbl);
  wrap.appendChild(row);

  let kidsEl = null;
  if (hasKids) {
    kidsEl = document.createElement('div');
    kidsEl.className = 'tn-kids';
    for (const c of node.children) kidsEl.appendChild(mkNode(c, depth+1));
    wrap.appendChild(kidsEl);
  }

  row.addEventListener('click', e => {
    e.stopPropagation();
    if (hasKids) {
      const op = kidsEl.classList.toggle('op');
      car.classList.toggle('op', op);
      car.textContent = op ? '\u25BC' : '\u25BA';
    }
    if (node.hasTypes || !hasKids) openGroup(node.id, node.name);
  });
  return wrap;
}

/* ── Type list ── */
$('tl-back').addEventListener('click', () => {
  $('tlv').classList.remove('on');
  $('tv').classList.add('on');
});

async function openGroup(gid, gname) {
  $('tv').classList.remove('on');
  $('tlv').classList.add('on');
  $('tl-name').textContent = gname;
  const body = $('tl-body');
  body.innerHTML = '<div class="msg"><span class="sp"></span>Loading\u2026</div>';
  try {
    const types = await fetch('/api/types/' + gid).then(r => r.json());
    types.sort((a, b) => a.name.localeCompare(b.name));
    if (!types.length) { body.innerHTML = '<div class="msg">No items in this group</div>'; return; }
    body.innerHTML = '';
    for (const t of types) {
      const row = document.createElement('div');
      row.className = 'trow';
      if (t.id === S.tid) row.classList.add('sel');
      row.textContent = t.name;
      row.addEventListener('click', () => {
        body.querySelectorAll('.trow').forEach(r => r.classList.remove('sel'));
        row.classList.add('sel');
        selectType(t.id, t.name);
      });
      body.appendChild(row);
    }
  } catch(e) { body.innerHTML = '<div class="msg">Error loading items</div>'; }
}

/* ── Item selection ── */
async function selectType(tid, tname) {
  S.tid = tid; S.orders = [];
  $('mp-empty').style.display = 'none';
  $('mp-item').classList.add('on');
  $('iname').textContent = tname;
  $('ipath').textContent = '';
  $('ii').src = 'https://images.evetech.net/types/' + tid + '/icon?size=64';
  $('ii').onerror = function() { this.src = ''; };
  ['st-sl','st-by','st-mg','st-sv','st-bv','st-or'].forEach(id => $(id).textContent = '\u2014');
  $('sl-body').innerHTML = '<tr><td colspan="5" class="msg"><span class="sp"></span>Loading\u2026</td></tr>';
  $('by-body').innerHTML = '<tr><td colspan="7" class="msg"><span class="sp"></span>Loading\u2026</td></tr>';
  $('sl-meta').textContent = ''; $('by-meta').textContent = '';
  clearChart();

  try {
    const [orders] = await Promise.all([
      fetch('/api/orders/' + tid).then(r => r.json()),
      loadHistory(tid),
    ]);
    if (S.tid !== tid) return;
    S.orders = orders;

    /* batch-resolve ALL location names before first render */
    const locIds = [...new Set(orders.map(o => o.location_id).filter(Boolean))];
    const unknown = locIds.filter(id => !(id in S.names));
    if (unknown.length) {
      try {
        const nr = await fetch('/api/names?ids=' + unknown.join(','));
        if (nr.ok) Object.assign(S.names, await nr.json());
      } catch(e) {}
    }
    if (S.tid !== tid) return;

    computeStats();
    renderTables();
  } catch(err) {
    $('sl-body').innerHTML = '<tr><td colspan="5" class="msg">Error: '+esc(err.message)+'</td></tr>';
    $('by-body').innerHTML = '<tr><td colspan="7" class="msg">Error: '+esc(err.message)+'</td></tr>';
  }
}

/* ── Stats bar ── */
function computeStats() {
  const rid = regionId();
  const ord = rid ? S.orders.filter(o => o.region_id === rid) : S.orders;
  const sp  = ord.filter(o => !o.is_buy_order).map(o => o.price).sort((a,b) => a-b);
  const bp  = ord.filter(o =>  o.is_buy_order).map(o => o.price).sort((a,b) => a-b);
  const bs  = pct(sp, 0.05), bb = pct(bp, 0.95);
  const mg  = bs && bb && bs > bb ? ((bs - bb) / bs * 100) : null;
  const sv  = ord.filter(o => !o.is_buy_order).reduce((s,o) => s + o.volume_remain, 0);
  const bv  = ord.filter(o =>  o.is_buy_order).reduce((s,o) => s + o.volume_remain, 0);
  $('st-sl').textContent = bs ? fmtISK(bs) : '\u2014';
  $('st-by').textContent = bb ? fmtISK(bb) : '\u2014';
  $('st-mg').textContent = mg != null ? mg.toFixed(1)+'%' : '\u2014';
  $('st-sv').textContent = fmtN(sv);
  $('st-bv').textContent = fmtN(bv);
  $('st-or').textContent = sp.length + ' / ' + bp.length;
}

/* ── Tables ── */
function renderTables() {
  const rid = regionId();
  let sl = S.orders.filter(o => !o.is_buy_order);
  let by = S.orders.filter(o =>  o.is_buy_order);
  if (rid) { sl = sl.filter(o => o.region_id === rid); by = by.filter(o => o.region_id === rid); }
  sl = srt(sl, S.sort.s); by = srt(by, S.sort.b);
  $('sl-meta').textContent = sl.length + ' orders';
  $('by-meta').textContent = by.length + ' orders';
  renderBody('sl-body', sl, false, 5);
  renderBody('by-body', by, true,  7);
  updateSortUI('s'); updateSortUI('b');
}

function srt(arr, st) {
  const {c, d} = st;
  return [...arr].sort((a, b) => {
    let va, vb;
    if (c === 'loc') {
      va = S.names[a.location_id] || String(a.location_id);
      vb = S.names[b.location_id] || String(b.location_id);
    } else if (c === 'sec') {
      va = a.security_status ?? -1; vb = b.security_status ?? -1;
    } else if (c === 'exp') {
      va = a.issued ? new Date(a.issued).getTime() + (a.duration||0)*86400000 : 0;
      vb = b.issued ? new Date(b.issued).getTime() + (b.duration||0)*86400000 : 0;
    } else {
      va = a[c] ?? 0; vb = b[c] ?? 0;
    }
    if (typeof va === 'string') return d === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
    return d === 'asc' ? va - vb : vb - va;
  });
}

function renderBody(bodyId, rows, isBuy, cols) {
  const body = $(bodyId);
  if (!rows.length) {
    body.innerHTML = '<tr><td colspan="'+cols+'" class="msg">No orders.</td></tr>'; return;
  }
  body.innerHTML = rows.slice(0, 800).map(o => {
    const ln  = esc(S.names[o.location_id] || String(o.location_id));
    const rn  = esc(o.region_name || '');
    const sc  = secCls(o.security_status);
    const sv  = fmtSec(o.security_status);
    const ex  = fmtExp(o.issued, o.duration);
    const lc  = '<td><span class="loc">'+ln+'</span> <span class="rgn">'+rn+'</span></td>';
    if (isBuy) return '<tr>'+
      '<td class="pb">'+fmtISK(o.price)+'</td>'+
      '<td>'+fmtN(o.volume_remain)+'</td>'+
      '<td>'+esc(o.range||'')+'</td>'+
      '<td>'+fmtN(o.min_volume)+'</td>'+
      lc+
      '<td class="'+sc+'">'+sv+'</td>'+
      '<td>'+ex+'</td></tr>';
    return '<tr>'+
      '<td class="ps">'+fmtISK(o.price)+'</td>'+
      '<td>'+fmtN(o.volume_remain)+'</td>'+
      lc+
      '<td class="'+sc+'">'+sv+'</td>'+
      '<td>'+ex+'</td></tr>';
  }).join('');
  if (rows.length > 800) {
    body.innerHTML += '<tr><td colspan="'+cols+'" class="msg">Showing 800 of '+rows.length+' orders</td></tr>';
  }
}

/* Sort header clicks */
document.querySelectorAll('thead th[data-c]').forEach(th => {
  th.addEventListener('click', () => {
    const c = th.dataset.c, t = th.dataset.t;
    const st = S.sort[t];
    if (st.c === c) st.d = st.d === 'asc' ? 'desc' : 'asc';
    else { st.c = c; st.d = (c==='price' && t==='s') ? 'asc' : (c==='price' && t==='b') ? 'desc' : 'asc'; }
    renderTables();
  });
});

function updateSortUI(t) {
  const st  = S.sort[t];
  const hdr = $(t === 's' ? 'sl-hdr' : 'by-hdr');
  hdr.querySelectorAll('th').forEach(th => {
    th.classList.remove('asc','desc');
    if (th.dataset.c === st.c) th.classList.add(st.d);
  });
}

/* ── History chart ── */
function clearChart() {
  if (S.chart) { try { S.chart.destroy(); } catch(e) {} S.chart = null; }
  $('ch-el').innerHTML = '<div class="msg">No history data available.</div>';
  $('ch-meta').textContent = '';
}

async function loadHistory(tid) {
  const rid = regionId();
  const url = '/api/esi_history/' + tid + '/' + (rid || 'all');
  clearChart();
  try {
    const data = await fetch(url).then(r => r.json());
    if (S.tid !== tid) return;
    const rows = (data.rows || [])
      .filter(r => r.date && r.average != null)
      .sort((a,b) => a.date.localeCompare(b.date));
    if (!rows.length) {
      $('ch-el').innerHTML = '<div class="msg">No cached history for this item/region yet.</div>';
      return;
    }
    $('ch-meta').textContent = rows.length + ' days';
    drawChart(rows);
  } catch(e) {
    $('ch-el').innerHTML = '<div class="msg">History unavailable.</div>';
  }
}

function drawChart(rows) {
  const el = $('ch-el'); el.innerHTML = '';
  const W  = Math.max(300, el.clientWidth - 16);
  const xs = rows.map(r => new Date(r.date + 'T00:00:00Z').getTime() / 1000);
  const hi = rows.map(r => r.highest || null);
  const av = rows.map(r => r.average  || null);
  const lo = rows.map(r => r.lowest   || null);
  const vo = rows.map(r => r.volume   || 0);
  const AX = '#354060', GR = 'rgba(40,50,80,0.7)';
  const opts = {
    width: W, height: 210,
    padding: [6, 10, 0, 0],
    legend: { show: false },
    cursor: { show: true },
    scales: { x: { time: true }, isk: {}, vol: {} },
    axes: [
      { stroke: AX, ticks: {stroke:AX}, grid: {stroke:GR}, font: '11px sans-serif' },
      { scale:'isk', stroke:AX, ticks:{stroke:AX}, grid:{stroke:GR}, font:'11px sans-serif',
        size: 70, values: (u,vs) => vs.map(v => v==null?'':fmtISK(v)) },
      { scale:'vol', side:1, stroke:AX, ticks:{stroke:AX}, grid:{stroke:GR},
        font:'11px sans-serif', size:55, values: (u,vs) => vs.map(v => v==null?'':fmtN(v)) },
    ],
    series: [
      {},
      { label:'High', scale:'isk', stroke:'#506838', width:1,
        paths: uPlot.paths.linear(), points:{show:false} },
      { label:'Avg',  scale:'isk', stroke:'#3e7cb5', width:2,
        paths: uPlot.paths.linear(), points:{show:false} },
      { label:'Low',  scale:'isk', stroke:'#705090', width:1,
        paths: uPlot.paths.linear(), points:{show:false} },
      { label:'Vol',  scale:'vol', stroke:'rgba(62,124,181,0.5)', fill:'rgba(62,124,181,0.1)',
        width:1, paths: uPlot.paths.bars({size:[0.6,20]}), points:{show:false} },
    ],
  };
  try { S.chart = new uPlot(opts, [xs, hi, av, lo, vo], el); }
  catch(e) { el.innerHTML = '<div class="msg">Chart error: '+esc(e.message)+'</div>'; }
}
</script>
</body>
</html>"""


def make_browser_html(tree: list, regions: list) -> bytes:
    """Return the browser page with SDE data embedded as JS constants."""
    return (
        _TMPL
        .replace("/*TREE*/", json.dumps(tree, separators=(",", ":")))
        .replace("/*REGIONS*/", json.dumps(regions, separators=(",", ":")))
        .encode("utf-8")
    )
