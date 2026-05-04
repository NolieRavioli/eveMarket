"""Browser UI HTML — served at GET /browser."""

BROWSER_HTML: bytes = b"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>EVE Market Browser</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/uplot@1.6.31/dist/uPlot.min.css">
<style>
:root{
  --bg:#0a0d14;--bg2:#10141f;--bg3:#161b2e;--bg4:#1a2035;
  --border:#2a3050;--border2:#3a4060;
  --text:#b0bcd4;--text2:#7a8aaa;--head:#d0ddf0;
  --accent:#4a90d9;--accent2:#6aaeff;
  --sell:#e05050;--sell2:#ff7070;
  --buy:#40a850;--buy2:#60c870;
  --warn:#d09030;
  --sidebar:290px;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;overflow:hidden;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:13px}

/* Layout */
#app{display:flex;height:100vh}
#sidebar{width:var(--sidebar);min-width:var(--sidebar);display:flex;flex-direction:column;background:var(--bg2);border-right:1px solid var(--border);overflow:hidden}
#main{flex:1;display:flex;flex-direction:column;overflow:hidden}

/* Sidebar search */
#search-wrap{padding:10px;border-bottom:1px solid var(--border);flex-shrink:0}
#search-input{width:100%;background:var(--bg3);border:1px solid var(--border2);border-radius:4px;padding:6px 10px;color:var(--head);font-size:13px;outline:none}
#search-input:focus{border-color:var(--accent)}
#search-input::placeholder{color:var(--text2)}

/* Tree */
#tree-wrap{flex:1;overflow-y:auto;overflow-x:hidden}
#tree-list-wrap{display:none;border-top:1px solid var(--border);flex-shrink:0;overflow-y:auto;max-height:45%}
#type-list-header{padding:6px 10px;font-size:11px;color:var(--text2);background:var(--bg3);border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}
#type-list-header button{background:none;border:none;color:var(--text2);cursor:pointer;font-size:14px;line-height:1}
#type-list{overflow-y:auto;flex:1}
.type-item{padding:5px 12px;cursor:pointer;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:var(--text)}
.type-item:hover{background:var(--bg4);color:var(--head)}
.type-item.active{background:var(--bg4);color:var(--accent2);font-weight:600}

/* Tree nodes */
.tree-node{user-select:none}
.tree-row{display:flex;align-items:center;padding:3px 6px;cursor:pointer;white-space:nowrap;overflow:hidden}
.tree-row:hover{background:var(--bg4)}
.tree-row.active-group{color:var(--accent2)}
.tree-toggle{color:var(--text2);font-size:10px;width:14px;flex-shrink:0;transition:transform 0.1s}
.tree-toggle.open{transform:rotate(90deg)}
.tree-name{overflow:hidden;text-overflow:ellipsis;flex:1}
.tree-children{display:none}
.tree-children.open{display:block}

/* Main tabs / panels */
#empty-state{flex:1;display:flex;align-items:center;justify-content:center;color:var(--text2);font-size:14px;flex-direction:column;gap:8px}
#empty-state .hint{font-size:12px;color:var(--border2)}
#item-panel{flex:1;display:none;flex-direction:column;overflow:hidden}

/* Item header */
#item-header{display:flex;align-items:center;gap:12px;padding:12px 16px;background:var(--bg2);border-bottom:1px solid var(--border);flex-shrink:0}
#item-icon{width:48px;height:48px;border-radius:4px;background:var(--bg3);flex-shrink:0}
#item-meta{min-width:0}
#item-breadcrumb{font-size:11px;color:var(--text2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
#item-name{font-size:18px;font-weight:600;color:var(--head);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}

/* Scrollable content */
#content-scroll{flex:1;overflow-y:auto;padding:12px 16px;display:flex;flex-direction:column;gap:14px}

/* Stats cards */
#stats-cards{display:flex;gap:10px;flex-wrap:wrap}
.stat-card{background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:8px 12px;min-width:110px;flex:1}
.stat-label{font-size:10px;text-transform:uppercase;letter-spacing:.06em;color:var(--text2);margin-bottom:3px}
.stat-val{font-size:15px;font-weight:600;color:var(--head)}
.stat-val.sell-c{color:var(--sell2)}
.stat-val.buy-c{color:var(--buy2)}
.stat-val.neutral{color:var(--text)}

/* Section cards */
.section{background:var(--bg2);border:1px solid var(--border);border-radius:6px;overflow:hidden}
.section-header{padding:8px 12px;background:var(--bg3);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap}
.section-title{font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:var(--text2)}
.section-controls{display:flex;gap:8px;align-items:center;flex-wrap:wrap}

/* Form controls */
select,input[type=text],input[type=number]{background:var(--bg3);border:1px solid var(--border2);border-radius:3px;padding:3px 7px;color:var(--head);font-size:12px;outline:none}
select:focus,input:focus{border-color:var(--accent)}
select option{background:var(--bg2)}
label{font-size:11px;color:var(--text2)}

/* Chart */
#chart-wrap{padding:10px 12px;min-height:200px}
.uplot canvas{display:block}

/* Orders tables */
.orders-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse}
thead th{padding:6px 10px;text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--text2);border-bottom:1px solid var(--border);position:sticky;top:0;background:var(--bg3);cursor:pointer;user-select:none;white-space:nowrap}
thead th:hover{color:var(--head)}
thead th .sort-arrow{opacity:.4;margin-left:4px;font-size:9px}
thead th.asc .sort-arrow::after{content:'▲'}
thead th.desc .sort-arrow::after{content:'▼'}
thead th:not(.asc):not(.desc) .sort-arrow::after{content:'⇅'}
tbody tr{border-bottom:1px solid var(--border)}
tbody tr:hover{background:var(--bg4)}
tbody td{padding:5px 10px;white-space:nowrap}
.price-sell{color:var(--sell2)}
.price-buy{color:var(--buy2)}
.sec-high{color:#5bc85b}
.sec-med{color:#f0c040}
.sec-low{color:#e07030}
.sec-null{color:#c03030}
.loading{padding:20px;text-align:center;color:var(--text2)}
.no-data{padding:14px;text-align:center;color:var(--text2);font-size:12px}

/* Filter bar */
#filter-bar{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
#filter-bar label{display:flex;align-items:center;gap:4px}
#filter-bar input[type=text]{width:130px}
#filter-bar input[type=number]{width:70px}

/* Scrollbars */
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:var(--bg2)}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:3px}

/* Loading spinner */
.spinner{display:inline-block;width:14px;height:14px;border:2px solid var(--border2);border-top-color:var(--accent);border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle;margin-right:6px}
@keyframes spin{to{transform:rotate(360deg)}}

/* Responsive: hide sidebar label on very small */
@media(max-width:600px){:root{--sidebar:200px}}
</style>
</head>
<body>
<div id="app">

  <!-- ===== SIDEBAR ===== -->
  <aside id="sidebar">
    <div id="search-wrap">
      <input id="search-input" type="text" placeholder="&#128269; Search items...">
    </div>
    <div id="tree-wrap">
      <div id="tree-container"></div>
    </div>
    <div id="tree-list-wrap">
      <div id="type-list-header">
        <span id="type-list-title">Items</span>
        <button id="type-list-close" title="Close">&times;</button>
      </div>
      <div id="type-list"></div>
    </div>
  </aside>

  <!-- ===== MAIN ===== -->
  <main id="main">
    <div id="empty-state">
      <div>&#9432; Select an item from the left panel</div>
      <div class="hint">Market group tree &rarr; item group &rarr; click an item</div>
    </div>

    <div id="item-panel">
      <!-- Item header -->
      <div id="item-header">
        <img id="item-icon" src="" alt="">
        <div id="item-meta">
          <div id="item-breadcrumb"></div>
          <div id="item-name">Loading...</div>
        </div>
        <div id="item-loading" style="margin-left:auto;display:none"><span class="spinner"></span></div>
      </div>

      <!-- Scrollable content area -->
      <div id="content-scroll">

        <!-- Stats cards -->
        <div id="stats-cards">
          <div class="stat-card"><div class="stat-label">Best Sell</div><div class="stat-val sell-c" id="st-sell">&mdash;</div></div>
          <div class="stat-card"><div class="stat-label">Best Buy</div><div class="stat-val buy-c" id="st-buy">&mdash;</div></div>
          <div class="stat-card"><div class="stat-label">Spread</div><div class="stat-val neutral" id="st-spread">&mdash;</div></div>
          <div class="stat-card"><div class="stat-label">Sell Volume</div><div class="stat-val neutral" id="st-svol">&mdash;</div></div>
          <div class="stat-card"><div class="stat-label">Buy Volume</div><div class="stat-val neutral" id="st-bvol">&mdash;</div></div>
          <div class="stat-card"><div class="stat-label">Orders</div><div class="stat-val neutral" id="st-orders">&mdash;</div></div>
        </div>

        <!-- Price History -->
        <div class="section">
          <div class="section-header">
            <span class="section-title">Price History</span>
            <div class="section-controls">
              <label>Region:
                <select id="hist-region">
                  <option value="all">All Regions</option>
                </select>
              </label>
              <label>Or station (regex):
                <input type="text" id="hist-station" placeholder="Jita|Amarr...">
              </label>
            </div>
          </div>
          <div id="chart-wrap">
            <div class="no-data" id="chart-empty">No history data cached for this item/region.</div>
          </div>
        </div>

        <!-- Orders filter bar -->
        <div class="section">
          <div class="section-header">
            <span class="section-title">Order Filters</span>
            <div id="filter-bar">
              <label>Region:
                <select id="f-region"><option value="">All</option></select>
              </label>
              <label>Station (regex):
                <input type="text" id="f-station" placeholder="Jita|Amarr...">
              </label>
              <label>Max price:
                <input type="number" id="f-maxprice" placeholder="ISK" min="0" step="any">
              </label>
              <label>Min qty:
                <input type="number" id="f-minqty" placeholder="units" min="1" step="1">
              </label>
            </div>
          </div>
        </div>

        <!-- Sell orders -->
        <div class="section">
          <div class="section-header">
            <span class="section-title" style="color:var(--sell2)">&#9660; Sell Orders</span>
            <span id="sell-count" style="font-size:11px;color:var(--text2)"></span>
          </div>
          <div class="orders-wrap">
            <table id="sell-table">
              <thead>
                <tr>
                  <th data-col="price" data-tbl="sell">Price <span class="sort-arrow"></span></th>
                  <th data-col="volume_remain" data-tbl="sell">Qty <span class="sort-arrow"></span></th>
                  <th data-col="region_name" data-tbl="sell">Region <span class="sort-arrow"></span></th>
                  <th data-col="location_id" data-tbl="sell">Location <span class="sort-arrow"></span></th>
                  <th data-col="security_status" data-tbl="sell">Sec <span class="sort-arrow"></span></th>
                  <th data-col="issued" data-tbl="sell">Expires <span class="sort-arrow"></span></th>
                </tr>
              </thead>
              <tbody id="sell-body"><tr><td colspan="6" class="no-data">Select an item to view orders.</td></tr></tbody>
            </table>
          </div>
        </div>

        <!-- Buy orders -->
        <div class="section">
          <div class="section-header">
            <span class="section-title" style="color:var(--buy2)">&#9650; Buy Orders</span>
            <span id="buy-count" style="font-size:11px;color:var(--text2)"></span>
          </div>
          <div class="orders-wrap">
            <table id="buy-table">
              <thead>
                <tr>
                  <th data-col="price" data-tbl="buy">Price <span class="sort-arrow"></span></th>
                  <th data-col="volume_remain" data-tbl="buy">Qty <span class="sort-arrow"></span></th>
                  <th data-col="range" data-tbl="buy">Range <span class="sort-arrow"></span></th>
                  <th data-col="min_volume" data-tbl="buy">Min Vol <span class="sort-arrow"></span></th>
                  <th data-col="region_name" data-tbl="buy">Region <span class="sort-arrow"></span></th>
                  <th data-col="location_id" data-tbl="buy">Location <span class="sort-arrow"></span></th>
                  <th data-col="security_status" data-tbl="buy">Sec <span class="sort-arrow"></span></th>
                  <th data-col="issued" data-tbl="buy">Expires <span class="sort-arrow"></span></th>
                </tr>
              </thead>
              <tbody id="buy-body"><tr><td colspan="8" class="no-data">Select an item to view orders.</td></tr></tbody>
            </table>
          </div>
        </div>

      </div><!-- /content-scroll -->
    </div><!-- /item-panel -->
  </main>
</div>

<script src="https://cdn.jsdelivr.net/npm/uplot@1.6.31/dist/uPlot.iife.min.js"></script>
<script>
'use strict';

// ============================================================
// State
// ============================================================
let allRegions = [];
let allOrders = [];        // raw orders for current type
let filteredSells = [];
let filteredBuys = [];
let currentTypeId = null;
let currentTypeName = '';
let nameCache = {};        // id -> name string
let nameQueue = new Set(); // ids waiting for batch resolve
let nameResolveTimer = null;
let chartInst = null;
let sortState = {sell:{col:'price',dir:'asc'}, buy:{col:'price',dir:'desc'}};

// ============================================================
// Utility
// ============================================================
function fmt(n, decimals=2){
  if(n==null||isNaN(n))return '\u2014';
  if(n>=1e12)return(n/1e12).toFixed(2)+'T';
  if(n>=1e9)return(n/1e9).toFixed(2)+'B';
  if(n>=1e6)return(n/1e6).toFixed(2)+'M';
  if(n>=1e3)return(n/1e3).toFixed(2)+'K';
  return n.toFixed(decimals);
}
function fmtISK(n){
  if(n==null||isNaN(n))return '\u2014';
  return fmt(n)+' ISK';
}
function fmtNum(n){
  if(n==null||isNaN(n))return '\u2014';
  return n.toLocaleString();
}
function fmtDate(s){
  if(!s)return '';
  return s.slice(0,10);
}
function secColor(sec){
  if(sec==null)return 'sec-null';
  if(sec>=0.5)return 'sec-high';
  if(sec>=0.1)return 'sec-med';
  if(sec>0)return 'sec-low';
  return 'sec-null';
}
function fmtSec(sec){
  if(sec==null)return '?';
  const v=Math.max(0,Math.min(1,sec));
  return v.toFixed(1);
}
function expiresDate(issued, durationDays){
  if(!issued)return '';
  const d=new Date(issued);
  d.setDate(d.getDate()+(durationDays||0));
  return d.toISOString().slice(0,10);
}

// ============================================================
// Name resolution via /api/names
// ============================================================
function resolveNames(ids, callback){
  // Check cache first
  const need=ids.filter(id=>!(id in nameCache));
  if(!need.length){callback(ids.map(id=>nameCache[id]||String(id)));return;}
  need.forEach(id=>nameQueue.add(id));
  if(nameResolveTimer)clearTimeout(nameResolveTimer);
  nameResolveTimer=setTimeout(flushNameQueue,400);
  // Return immediately with IDs; caller can re-render when names arrive
  callback(ids.map(id=>nameCache[id]||String(id)));
}
async function flushNameQueue(){
  const ids=[...nameQueue];nameQueue.clear();
  if(!ids.length)return;
  try{
    const resp=await fetch('/api/names?ids='+ids.join(','));
    if(!resp.ok)return;
    const map=await resp.json();
    Object.assign(nameCache,map);
    // Re-render if we have orders loaded
    if(allOrders.length)applyFiltersAndRender();
  }catch(e){}
}

// ============================================================
// Tree rendering
// ============================================================
function renderTree(groups){
  const container=document.getElementById('tree-container');
  container.innerHTML='';
  for(const g of groups) container.appendChild(makeTreeNode(g,0));
}

function makeTreeNode(node,depth){
  const div=document.createElement('div');
  div.className='tree-node';
  const row=document.createElement('div');
  row.className='tree-row';
  row.style.paddingLeft=(8+depth*14)+'px';

  const hasChildren=node.children&&node.children.length>0;
  const toggle=document.createElement('span');
  toggle.className='tree-toggle';
  toggle.textContent=hasChildren?'\u25BA':'\u00A0';
  const name=document.createElement('span');
  name.className='tree-name';
  name.textContent=node.name;
  row.appendChild(toggle);
  row.appendChild(name);
  div.appendChild(row);

  let childrenDiv=null;
  if(hasChildren){
    childrenDiv=document.createElement('div');
    childrenDiv.className='tree-children';
    for(const child of node.children)
      childrenDiv.appendChild(makeTreeNode(child,depth+1));
    div.appendChild(childrenDiv);
    row.addEventListener('click',e=>{
      e.stopPropagation();
      const open=childrenDiv.classList.toggle('open');
      toggle.classList.toggle('open',open);
    });
  }

  // Group that might have types - clicking loads them
  row.addEventListener('click',e=>{
    if(hasChildren)return; // handled above
    loadGroupTypes(node.id,node.name);
    document.querySelectorAll('.tree-row.active-group')
      .forEach(r=>r.classList.remove('active-group'));
    row.classList.add('active-group');
  });
  // For parent nodes, also allow clicking name to open type list
  if(hasChildren){
    name.addEventListener('click',e=>{
      e.stopPropagation();
      loadGroupTypes(node.id,node.name);
      document.querySelectorAll('.tree-row.active-group')
        .forEach(r=>r.classList.remove('active-group'));
      row.classList.add('active-group');
    });
  }

  div.dataset.id=node.id;
  div.dataset.name=node.name.toLowerCase();
  return div;
}

function filterTree(query){
  const q=query.toLowerCase().trim();
  const nodes=document.querySelectorAll('.tree-node');
  nodes.forEach(node=>{
    if(!q){node.style.display='';return;}
    const name=node.dataset.name||'';
    node.style.display=name.includes(q)?'':'none';
  });
}

// ============================================================
// Type list panel
// ============================================================
let currentGroupTypes=[];

async function loadGroupTypes(groupId,groupName){
  const wrap=document.getElementById('tree-list-wrap');
  const listEl=document.getElementById('type-list');
  const titleEl=document.getElementById('type-list-title');
  wrap.style.display='flex';
  wrap.style.flexDirection='column';
  titleEl.textContent=groupName;
  listEl.innerHTML='<div class="loading"><span class="spinner"></span>Loading...</div>';

  try{
    const types=await fetch('/api/types/'+groupId).then(r=>r.json());
    currentGroupTypes=types;
    renderTypeList(types);
  }catch(e){
    listEl.innerHTML='<div class="no-data">Error loading items</div>';
  }
}

function renderTypeList(types, filter=''){
  const listEl=document.getElementById('type-list');
  const q=filter.toLowerCase().trim();
  const shown=q?types.filter(t=>t.name.toLowerCase().includes(q)):types;
  if(!shown.length){listEl.innerHTML='<div class="no-data">No items found</div>';return;}
  listEl.innerHTML='';
  for(const t of shown){
    const div=document.createElement('div');
    div.className='type-item';
    div.textContent=t.name;
    div.dataset.id=t.id;
    div.addEventListener('click',()=>{
      document.querySelectorAll('.type-item.active').forEach(el=>el.classList.remove('active'));
      div.classList.add('active');
      selectType(t.id,t.name);
    });
    listEl.appendChild(div);
  }
}

document.getElementById('type-list-close').addEventListener('click',()=>{
  document.getElementById('tree-list-wrap').style.display='none';
});

// ============================================================
// Search
// ============================================================
let searchTimer=null;
document.getElementById('search-input').addEventListener('input',e=>{
  clearTimeout(searchTimer);
  searchTimer=setTimeout(()=>{
    const q=e.target.value.toLowerCase().trim();
    filterTree(q);
    if(currentGroupTypes.length) renderTypeList(currentGroupTypes,q);
  },200);
});

// ============================================================
// Type selection
// ============================================================
async function selectType(typeId,typeName){
  currentTypeId=typeId;
  currentTypeName=typeName;
  allOrders=[];
  filteredSells=[];
  filteredBuys=[];

  // Show item panel
  document.getElementById('empty-state').style.display='none';
  document.getElementById('item-panel').style.display='flex';

  // Update header
  document.getElementById('item-name').textContent=typeName;
  document.getElementById('item-breadcrumb').textContent='';
  document.getElementById('item-icon').src=
    'https://images.evetech.net/types/'+typeId+'/icon?size=64';
  document.getElementById('item-icon').onerror=function(){this.src='';};
  document.getElementById('item-loading').style.display='block';

  // Reset stats
  ['st-sell','st-buy','st-spread','st-svol','st-bvol','st-orders'].forEach(id=>{
    document.getElementById(id).textContent='\u2014';
  });
  document.getElementById('sell-body').innerHTML=
    '<tr><td colspan="6" class="loading"><span class="spinner"></span>Loading...</td></tr>';
  document.getElementById('buy-body').innerHTML=
    '<tr><td colspan="8" class="loading"><span class="spinner"></span>Loading...</td></tr>';
  clearChart();

  try{
    const orders=await fetch('/api/orders/'+typeId).then(r=>r.json());
    if(currentTypeId!==typeId)return; // stale
    allOrders=orders;
    computeAndShowStats(orders);
    applyFiltersAndRender();
    // Load history for selected region or all
    const regionSel=document.getElementById('hist-region').value;
    await loadHistory(typeId,regionSel);
  }catch(err){
    document.getElementById('sell-body').innerHTML=
      '<tr><td colspan="6" class="no-data">Error: '+err.message+'</td></tr>';
    document.getElementById('buy-body').innerHTML=
      '<tr><td colspan="8" class="no-data">Error: '+err.message+'</td></tr>';
  }finally{
    document.getElementById('item-loading').style.display='none';
  }
}

// ============================================================
// Stats
// ============================================================
function pct(arr,p){
  if(!arr.length)return 0;
  const sorted=[...arr].sort((a,b)=>a-b);
  const idx=Math.max(0,Math.ceil(sorted.length*p)-1);
  return sorted[idx];
}
function computeAndShowStats(orders){
  const sells=orders.filter(o=>!o.is_buy_order);
  const buys=orders.filter(o=>o.is_buy_order);
  const sellPrices=sells.map(o=>o.price);
  const buyPrices=buys.map(o=>o.price);
  const sell5=pct(sellPrices,0.05);
  const buy95=pct(buyPrices,0.95);
  const spread=sell5&&buy95?((sell5-buy95)/sell5*100):null;
  const svol=sells.reduce((a,o)=>a+o.volume_remain,0);
  const bvol=buys.reduce((a,o)=>a+o.volume_remain,0);
  document.getElementById('st-sell').textContent=fmtISK(sell5);
  document.getElementById('st-buy').textContent=fmtISK(buy95);
  document.getElementById('st-spread').textContent=
    spread!=null?spread.toFixed(2)+'%':'\u2014';
  document.getElementById('st-svol').textContent=fmtNum(svol);
  document.getElementById('st-bvol').textContent=fmtNum(bvol);
  document.getElementById('st-orders').textContent=
    fmtNum(sells.length)+' sell / '+fmtNum(buys.length)+' buy';
}

// ============================================================
// Filters
// ============================================================
function applyFiltersAndRender(){
  const regionVal=document.getElementById('f-region').value;
  const stationRx=document.getElementById('f-station').value.trim();
  const maxPrice=parseFloat(document.getElementById('f-maxprice').value)||null;
  const minQty=parseInt(document.getElementById('f-minqty').value)||null;

  let stRx=null;
  try{if(stationRx)stRx=new RegExp(stationRx,'i');}catch(e){}

  function pass(o){
    if(regionVal&&String(o.region_id)!==regionVal)return false;
    if(stRx){
      const nm=nameCache[o.location_id]||String(o.location_id);
      if(!stRx.test(nm))return false;
    }
    if(maxPrice!=null&&o.price>maxPrice)return false;
    if(minQty!=null&&o.volume_remain<minQty)return false;
    return true;
  }

  filteredSells=allOrders.filter(o=>!o.is_buy_order&&pass(o));
  filteredBuys=allOrders.filter(o=>o.is_buy_order&&pass(o));

  // Resolve location names if not cached
  const locationIds=[...new Set(allOrders.map(o=>o.location_id).filter(Boolean))];
  const unknown=locationIds.filter(id=>!(id in nameCache));
  if(unknown.length){
    unknown.forEach(id=>nameQueue.add(id));
    if(nameResolveTimer)clearTimeout(nameResolveTimer);
    nameResolveTimer=setTimeout(flushNameQueue,200);
  }

  renderSellTable();
  renderBuyTable();
}

['f-region','f-station','f-maxprice','f-minqty'].forEach(id=>{
  const el=document.getElementById(id);
  el.addEventListener('change',()=>{if(allOrders.length)applyFiltersAndRender();});
  el.addEventListener('input',()=>{if(allOrders.length)applyFiltersAndRender();});
});

// ============================================================
// Sorting
// ============================================================
function sortOrders(orders,col,dir){
  return [...orders].sort((a,b)=>{
    let va=a[col],vb=b[col];
    if(col==='region_name'){va=a.region_name||'';vb=b.region_name||'';}
    if(col==='location_id'){
      va=nameCache[a.location_id]||String(a.location_id);
      vb=nameCache[b.location_id]||String(b.location_id);
    }
    if(va==null)va=dir==='asc'?Infinity:'';
    if(vb==null)vb=dir==='asc'?Infinity:'';
    if(typeof va==='string')return dir==='asc'?va.localeCompare(vb):vb.localeCompare(va);
    return dir==='asc'?va-vb:vb-va;
  });
}

document.querySelectorAll('thead th[data-col]').forEach(th=>{
  th.addEventListener('click',()=>{
    const col=th.dataset.col;
    const tbl=th.dataset.tbl;
    const st=sortState[tbl];
    if(st.col===col){st.dir=st.dir==='asc'?'desc':'asc';}
    else{st.col=col;st.dir=tbl==='sell'?'asc':'desc';}
    updateSortHeaders(tbl);
    if(tbl==='sell')renderSellTable();
    else renderBuyTable();
  });
});

function updateSortHeaders(tbl){
  const st=sortState[tbl];
  document.querySelectorAll('thead th[data-tbl="'+tbl+'"]').forEach(th=>{
    th.classList.remove('asc','desc');
    if(th.dataset.col===st.col)th.classList.add(st.dir);
  });
}

// ============================================================
// Sell table
// ============================================================
function renderSellTable(){
  const st=sortState.sell;
  const rows=sortOrders(filteredSells,st.col,st.dir);
  const tbody=document.getElementById('sell-body');
  document.getElementById('sell-count').textContent=rows.length+' orders';
  if(!rows.length){
    tbody.innerHTML='<tr><td colspan="6" class="no-data">No sell orders match the current filters.</td></tr>';
    return;
  }
  tbody.innerHTML=rows.slice(0,500).map(o=>{
    const loc=nameCache[o.location_id]||o.location_id;
    const sec=o.security_status;
    const cls=secColor(sec);
    const exp=expiresDate(o.issued,o.duration);
    return '<tr>'+
      '<td class="price-sell">'+fmtISK(o.price)+'</td>'+
      '<td>'+fmtNum(o.volume_remain)+'</td>'+
      '<td>'+(o.region_name||'')+'</td>'+
      '<td title="'+o.location_id+'">'+htmlEsc(String(loc))+'</td>'+
      '<td class="'+cls+'">'+fmtSec(sec)+'</td>'+
      '<td>'+htmlEsc(exp)+'</td>'+
      '</tr>';
  }).join('');
  if(rows.length>500){
    tbody.innerHTML+='<tr><td colspan="6" class="no-data">... and '+(rows.length-500)+' more (showing top 500)</td></tr>';
  }
}

// ============================================================
// Buy table
// ============================================================
function renderBuyTable(){
  const st=sortState.buy;
  const rows=sortOrders(filteredBuys,st.col,st.dir);
  const tbody=document.getElementById('buy-body');
  document.getElementById('buy-count').textContent=rows.length+' orders';
  if(!rows.length){
    tbody.innerHTML='<tr><td colspan="8" class="no-data">No buy orders match the current filters.</td></tr>';
    return;
  }
  tbody.innerHTML=rows.slice(0,500).map(o=>{
    const loc=nameCache[o.location_id]||o.location_id;
    const sec=o.security_status;
    const cls=secColor(sec);
    const exp=expiresDate(o.issued,o.duration);
    return '<tr>'+
      '<td class="price-buy">'+fmtISK(o.price)+'</td>'+
      '<td>'+fmtNum(o.volume_remain)+'</td>'+
      '<td>'+(o.range||'')+'</td>'+
      '<td>'+fmtNum(o.min_volume)+'</td>'+
      '<td>'+(o.region_name||'')+'</td>'+
      '<td title="'+o.location_id+'">'+htmlEsc(String(loc))+'</td>'+
      '<td class="'+cls+'">'+fmtSec(sec)+'</td>'+
      '<td>'+htmlEsc(exp)+'</td>'+
      '</tr>';
  }).join('');
  if(rows.length>500){
    tbody.innerHTML+='<tr><td colspan="8" class="no-data">... and '+(rows.length-500)+' more (showing top 500)</td></tr>';
  }
}

function htmlEsc(s){
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ============================================================
// History chart
// ============================================================
async function loadHistory(typeId, regionVal){
  clearChart();
  document.getElementById('chart-empty').textContent='Loading history...';
  document.getElementById('chart-empty').style.display='block';

  const histStation=document.getElementById('hist-station').value.trim();
  let url;
  if(histStation){
    // Station filter: use "all" history and filter client-side by station name regex
    // (We use region "all" and note station regex is cosmetic for now — ESI history is per region)
    url='/api/esi_history/'+typeId+'/all';
  } else if(regionVal==='all'||!regionVal){
    url='/api/esi_history/'+typeId+'/all';
  } else {
    url='/api/esi_history/'+typeId+'/'+regionVal;
  }

  try{
    const data=await fetch(url).then(r=>r.json());
    if(currentTypeId!==typeId)return;
    const rows=data.rows||[];
    if(!rows.length){
      document.getElementById('chart-empty').textContent='No history data cached for this item/region.';
      return;
    }
    document.getElementById('chart-empty').style.display='none';
    renderChart(rows);
  }catch(e){
    document.getElementById('chart-empty').textContent='Failed to load history: '+e.message;
  }
}

function clearChart(){
  if(chartInst){try{chartInst.destroy();}catch(e){}chartInst=null;}
  const wrap=document.getElementById('chart-wrap');
  const old=wrap.querySelector('.u-wrap');
  if(old)old.remove();
  document.getElementById('chart-empty').style.display='block';
}

function renderChart(rows){
  rows=rows.filter(r=>r.date&&r.average!=null).sort((a,b)=>a.date.localeCompare(b.date));
  if(!rows.length)return;

  const ts=rows.map(r=>new Date(r.date+'T00:00:00Z').getTime()/1000);
  const avg=rows.map(r=>r.average||null);
  const high=rows.map(r=>r.highest||null);
  const low=rows.map(r=>r.lowest||null);
  const vol=rows.map(r=>r.volume||0);

  const wrap=document.getElementById('chart-wrap');
  const W=wrap.clientWidth||800;

  // Muted EVE colours
  const COLORS={avg:'#4a90d9',high:'#d09030',low:'#7a8aaa',vol:'rgba(74,144,217,0.15)'};
  const TICK_COLOR='#3a4060';
  const LABEL_COLOR='#7a8aaa';
  const GRID_COLOR='rgba(58,64,96,0.6)';

  const opts={
    width:W,height:240,
    padding:[10,10,0,0],
    cursor:{show:true,sync:{key:'mkt'}},
    legend:{show:true,live:true},
    scales:{
      x:{time:true},
      isk:{auto:true,distr:1},
      vol:{auto:true,distr:1},
    },
    axes:[
      {
        stroke:LABEL_COLOR,
        ticks:{stroke:TICK_COLOR,width:1},
        grid:{stroke:GRID_COLOR,width:1},
      },
      {
        scale:'isk',
        stroke:LABEL_COLOR,
        ticks:{stroke:TICK_COLOR,width:1},
        grid:{stroke:GRID_COLOR,width:1},
        values:(u,vals)=>vals.map(v=>v==null?'':fmt(v)),
        size:80,
      },
      {
        scale:'vol',
        side:1,
        stroke:LABEL_COLOR,
        ticks:{stroke:TICK_COLOR,width:1},
        grid:{stroke:GRID_COLOR,width:1},
        values:(u,vals)=>vals.map(v=>v==null?'':fmt(v,0)),
        size:60,
      },
    ],
    series:[
      {},
      {
        label:'High',scale:'isk',
        stroke:COLORS.high,width:1,
        paths:uPlot.paths.linear(),
        points:{show:false},
      },
      {
        label:'Avg',scale:'isk',
        stroke:COLORS.avg,width:2,
        paths:uPlot.paths.linear(),
        points:{show:false},
      },
      {
        label:'Low',scale:'isk',
        stroke:COLORS.low,width:1,
        paths:uPlot.paths.linear(),
        points:{show:false},
      },
      {
        label:'Volume',scale:'vol',
        stroke:'rgba(74,144,217,0.7)',
        fill:'rgba(74,144,217,0.12)',
        width:1,
        paths:uPlot.paths.bars({size:[0.7,Infinity]}),
        points:{show:false},
      },
    ],
  };

  try{
    chartInst=new uPlot(opts,[ts,high,avg,low,vol],wrap);
    // Apply dark background
    wrap.querySelector('.u-wrap').style.background='transparent';
  }catch(e){
    document.getElementById('chart-empty').textContent='Chart error: '+e.message;
    document.getElementById('chart-empty').style.display='block';
  }
}

// History controls
document.getElementById('hist-region').addEventListener('change',e=>{
  if(currentTypeId)loadHistory(currentTypeId,e.target.value);
});
let histStationTimer=null;
document.getElementById('hist-station').addEventListener('input',()=>{
  clearTimeout(histStationTimer);
  histStationTimer=setTimeout(()=>{
    if(currentTypeId)loadHistory(currentTypeId,document.getElementById('hist-region').value);
  },600);
});

// ============================================================
// Init
// ============================================================
async function init(){
  try{
    const [groups,regions]=await Promise.all([
      fetch('/api/market_groups').then(r=>r.json()),
      fetch('/api/regions').then(r=>r.json()),
    ]);
    allRegions=regions;

    // Populate region dropdowns
    const histSel=document.getElementById('hist-region');
    const fSel=document.getElementById('f-region');
    for(const r of regions){
      const o1=document.createElement('option');
      o1.value=String(r.id);o1.textContent=r.name;
      histSel.appendChild(o1);
      const o2=document.createElement('option');
      o2.value=String(r.id);o2.textContent=r.name;
      fSel.appendChild(o2);
    }

    renderTree(groups);
  }catch(e){
    document.getElementById('tree-container').innerHTML=
      '<div class="no-data">Failed to load market data: '+e.message+'</div>';
  }
}

init();
</script>
</body>
</html>"""
