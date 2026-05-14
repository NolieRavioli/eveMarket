"""Appraisal-Maxxer: pick the single best station to buy-from / sell-to.

Given a multi-line paste of ``Name<sep>Qty`` lines (EVE multibuy format), find
the single station where:

  * mode="buy"  (Where to buy):  ALL items can be bought from sell orders,
                                  fully filling every requested quantity, at
                                  the lowest total ISK cost.
  * mode="sell" (Where to sell): ALL items can be sold into buy orders that
                                  are reachable from the seller station,
                                  fully filling every requested quantity, at
                                  the highest total ISK proceeds.

Buy-order range is honored: a "region" buy order is reachable from any seller
station in the same region; "solarsystem" only from the same system; etc.
Numeric jump-range orders are treated conservatively as same-system only.

This module is stateless aside from a small in-process name index built from
the SDE on first use.
"""
from __future__ import annotations

import re
import threading
from pathlib import Path
from typing import Iterable, Optional

from .index import iter_snapshot
from .location import LocationResolver
from .sde_market import get_cache
from .snapshot import latest_snapshot, orders_path


# ---------------------------------------------------------------------------
# SDE-derived lookups (built lazily, cached process-wide)
# ---------------------------------------------------------------------------
_LOCK = threading.Lock()
_NAME_INDEX: dict[str, int] | None = None
_CONS_OF_SYSTEM: dict[int, int] | None = None


def _name_index(sde_dir: Path) -> dict[str, int]:
    global _NAME_INDEX
    with _LOCK:
        if _NAME_INDEX is not None:
            return _NAME_INDEX
        cache = get_cache(sde_dir)
        idx: dict[str, int] = {}
        for tid, name in cache["type_names"].items():
            idx[name.strip().lower()] = int(tid)
        _NAME_INDEX = idx
        return _NAME_INDEX


def _cons_of_system(sde_dir: Path, resolver: LocationResolver) -> dict[int, int]:
    global _CONS_OF_SYSTEM
    with _LOCK:
        if _CONS_OF_SYSTEM is not None:
            return _CONS_OF_SYSTEM
        out: dict[int, int] = {}
        for cid, sids in resolver.systems_in_constellation.items():
            for s in sids:
                out[int(s)] = int(cid)
        _CONS_OF_SYSTEM = out
        return _CONS_OF_SYSTEM


# ---------------------------------------------------------------------------
# Paste parser
# ---------------------------------------------------------------------------
_QTY_RE = re.compile(r"^(.*?)[\s\t]+([\d,\.]+)\s*(?:x)?$")


def parse_paste(text: str) -> tuple[list[tuple[str, int]], list[str]]:
    """Parse a multibuy paste. Returns (items, errors).

    items: list of (name, qty) preserving paste order.
    errors: list of human-readable error strings for unparseable lines.
    """
    items: list[tuple[str, int]] = []
    errors: list[str] = []
    if not text:
        return items, errors

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        # Skip obvious total/header rows.
        low = line.lower()
        if low.startswith("total") or low.startswith("estimated"):
            continue

        # Tab-delimited (EVE in-game paste). Columns vary; we look for one
        # numeric column (qty) and take the first non-empty non-numeric
        # column as the name. Handles both "Name\tQty\t..." and
        # "\tQty\tName" layouts.
        if "\t" in line:
            cols = [c.strip() for c in line.split("\t")]
            qty: Optional[int] = None
            qty_idx: Optional[int] = None
            for i, c in enumerate(cols):
                if not c:
                    continue
                cs = c.replace(",", "")
                # Integer-only (don't accidentally consume a decimal price).
                if cs.isdigit():
                    try:
                        qty = int(cs)
                        qty_idx = i
                        break
                    except ValueError:
                        pass
            name = ""
            if qty_idx is not None:
                for i, c in enumerate(cols):
                    if i == qty_idx or not c or c == "-":
                        continue
                    cs = c.replace(",", "")
                    if cs.isdigit():
                        continue
                    name = c
                    break
            if not name or qty is None or qty <= 0:
                errors.append(f"could not parse: {raw!r}")
                continue
            items.append((name, qty))
            continue

        # Space-delimited: "Name with spaces 60"
        m = _QTY_RE.match(line)
        if m:
            name = m.group(1).strip()
            qty_str = m.group(2).replace(",", "")
            try:
                qty = int(float(qty_str))
            except ValueError:
                errors.append(f"could not parse qty: {raw!r}")
                continue
            if not name or qty <= 0:
                errors.append(f"could not parse: {raw!r}")
                continue
            items.append((name, qty))
            continue

        # Bare name → assume qty 1.
        items.append((line, 1))

    return items, errors


def resolve_names(sde_dir: Path, items: list[tuple[str, int]]
                  ) -> tuple[dict[int, int], list[str]]:
    """Map names → type_ids and merge quantities. Returns (req, unknown_names)."""
    idx = _name_index(sde_dir)
    req: dict[int, int] = {}
    unknown: list[str] = []
    for name, qty in items:
        tid = idx.get(name.strip().lower())
        if tid is None:
            unknown.append(name)
            continue
        req[tid] = req.get(tid, 0) + qty
    return req, unknown


# ---------------------------------------------------------------------------
# Snapshot scan
# ---------------------------------------------------------------------------
def _scan_snapshot(snap_path: Path, type_ids: set[int]):
    """One-pass scan: returns (sell_by_loc, buy_orders, loc_meta).

    sell_by_loc[loc_id][tid] = list[(price, volume_remain)] sorted asc by price
    buy_orders[tid] = list of dicts {price, vol, loc, sys, rgn, range}
    loc_meta[loc_id] = {"sys": ..., "rgn": ...}  populated from any order seen
    """
    sell_by_loc: dict[int, dict[int, list[tuple[float, int]]]] = {}
    buy_orders: dict[int, list[dict]] = {}
    loc_meta: dict[int, dict] = {}

    for o in iter_snapshot(snap_path):
        tid = o.get("type_id")
        if tid is None or int(tid) not in type_ids:
            continue
        tid = int(tid)
        loc = o.get("location_id")
        sys_id = o.get("system_id")
        rgn = o.get("region_id")
        if loc is not None:
            loc = int(loc)
            if loc not in loc_meta:
                loc_meta[loc] = {
                    "sys": int(sys_id) if sys_id is not None else None,
                    "rgn": int(rgn) if rgn is not None else None,
                }
        price = float(o.get("price") or 0.0)
        vol = int(o.get("volume_remain") or 0)
        if vol <= 0:
            continue
        if o.get("is_buy_order"):
            buy_orders.setdefault(tid, []).append({
                "price": price,
                "vol": vol,
                "loc": loc,
                "sys": int(sys_id) if sys_id is not None else None,
                "rgn": int(rgn) if rgn is not None else None,
                "range": str(o.get("range") or "").lower(),
            })
        else:
            if loc is None:
                continue
            sell_by_loc.setdefault(loc, {}).setdefault(tid, []).append((price, vol))

    # Sort each per-location sell list by price ascending (cheapest first).
    for per_loc in sell_by_loc.values():
        for lst in per_loc.values():
            lst.sort(key=lambda x: x[0])

    return sell_by_loc, buy_orders, loc_meta


def _fill_cost(offers: list[tuple[float, int]], qty_needed: int
               ) -> tuple[Optional[float], int]:
    """Walk price-ascending offers; return (total_cost, units_filled).

    total_cost is None if the offers cannot fully cover ``qty_needed``.
    """
    cost = 0.0
    remaining = qty_needed
    filled = 0
    for price, vol in offers:
        if remaining <= 0:
            break
        take = min(vol, remaining)
        cost += take * price
        remaining -= take
        filled += take
    if remaining > 0:
        return None, filled
    return cost, filled


def _fill_proceeds(offers: list[dict], qty_needed: int
                   ) -> tuple[Optional[float], int]:
    """Walk price-descending buy offers; return (proceeds, units_sold)."""
    proceeds = 0.0
    remaining = qty_needed
    sold = 0
    # Caller passes already-sorted offers (desc by price).
    for o in offers:
        if remaining <= 0:
            break
        take = min(o["vol"], remaining)
        proceeds += take * o["price"]
        remaining -= take
        sold += take
    if remaining > 0:
        return None, sold
    return proceeds, sold


# ---------------------------------------------------------------------------
# Where to buy
# ---------------------------------------------------------------------------
def best_buy_locations(req: dict[int, int],
                        sell_by_loc: dict[int, dict[int, list[tuple[float, int]]]],
                        top_n: int = 10):
    """Return top-N stations where ALL items can be bought from sell orders."""
    needed_types = set(req)
    results = []
    for loc_id, per_type in sell_by_loc.items():
        if not needed_types.issubset(per_type.keys()):
            continue
        total = 0.0
        breakdown = []
        ok = True
        for tid, qty in req.items():
            cost, filled = _fill_cost(per_type[tid], qty)
            if cost is None:
                ok = False
                break
            total += cost
            best_unit = per_type[tid][0][0] if per_type[tid] else 0.0
            breakdown.append({
                "type_id": tid,
                "qty": qty,
                "filled": filled,
                "cost": cost,
                "best_unit_price": best_unit,
                "avg_unit_price": (cost / qty) if qty else 0.0,
            })
        if not ok:
            continue
        results.append({"location_id": loc_id, "total": total,
                         "items": breakdown})
    results.sort(key=lambda r: r["total"])
    return results[:top_n]


# ---------------------------------------------------------------------------
# Where to sell
# ---------------------------------------------------------------------------
def _is_reachable(order: dict, seller_loc: int,
                  seller_sys: Optional[int], seller_con: Optional[int],
                  seller_rgn: Optional[int]) -> bool:
    """Can a seller standing in (seller_loc, seller_sys, ...) hit this buy order?"""
    r = order["range"]
    if r == "station":
        return order["loc"] is not None and order["loc"] == seller_loc
    if r == "solarsystem":
        return order["sys"] is not None and seller_sys is not None \
            and order["sys"] == seller_sys
    if r == "constellation":
        if order["sys"] is None or seller_con is None:
            return False
        # Look up constellation of the order's system from the same map.
        # Caller passes seller_con; we need the order's constellation.
        # We store this in order["con"] (filled by caller).
        return order.get("con") == seller_con
    if r == "region":
        return order["rgn"] is not None and seller_rgn is not None \
            and order["rgn"] == seller_rgn
    # Numeric jumps (1..40) — conservative: same-system only.
    if r.isdigit():
        return order["sys"] is not None and seller_sys is not None \
            and order["sys"] == seller_sys
    return False


def best_sell_locations(req: dict[int, int],
                         buy_orders: dict[int, list[dict]],
                         loc_meta: dict[int, dict],
                         resolver: LocationResolver,
                         cons_of_system: dict[int, int],
                         top_n: int = 10):
    """Return top-N stations where ALL items can be sold to buy orders."""
    # Annotate every buy order with its constellation for fast comparison.
    for tid, orders in buy_orders.items():
        for o in orders:
            if "con" in o:
                continue
            sys = o["sys"]
            o["con"] = cons_of_system.get(sys) if sys is not None else None

    # Candidate seller locations: every distinct station that hosts at least
    # one buy order for any requested item. Trade-hub-biased but well-bounded.
    candidates: set[int] = set()
    for orders in buy_orders.values():
        for o in orders:
            if o["loc"] is not None:
                candidates.add(o["loc"])

    results = []
    needed_types = set(req)
    for loc_id in candidates:
        meta = loc_meta.get(loc_id, {})
        seller_sys = meta.get("sys")
        seller_rgn = meta.get("rgn")
        seller_con = cons_of_system.get(seller_sys) if seller_sys else None

        total = 0.0
        breakdown = []
        ok = True
        for tid in needed_types:
            qty = req[tid]
            reachable = [o for o in buy_orders.get(tid, [])
                         if _is_reachable(o, loc_id, seller_sys,
                                          seller_con, seller_rgn)]
            if not reachable:
                ok = False
                break
            reachable.sort(key=lambda x: x["price"], reverse=True)
            proceeds, sold = _fill_proceeds(reachable, qty)
            if proceeds is None:
                ok = False
                break
            top_unit = reachable[0]["price"]
            breakdown.append({
                "type_id": tid,
                "qty": qty,
                "filled": sold,
                "proceeds": proceeds,
                "top_unit_price": top_unit,
                "avg_unit_price": (proceeds / qty) if qty else 0.0,
            })
        if not ok:
            continue
        results.append({"location_id": loc_id, "total": total + sum(
            b["proceeds"] for b in breakdown), "items": breakdown})
    # Replace placeholder; recompute totals cleanly.
    for r in results:
        r["total"] = sum(b["proceeds"] for b in r["items"])
    results.sort(key=lambda r: r["total"], reverse=True)
    return results[:top_n]


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def appraise(sde_dir: Path, data_dir: Path, resolver: LocationResolver,
             text: str, num_orders: int, mode: str,
             top_n: int = 10) -> dict:
    """Top-level: parse paste, scan snapshot, return ranked locations.

    Returns dict with:
      mode, num_orders, items (list of {name, qty, type_id, total_qty}),
      unknown (list of unparsed names), parse_errors (list[str]),
      results (list of {location_id, location_name?, total, items[...]}),
      snapshot_unix
    """
    parsed, parse_errors = parse_paste(text)
    if not parsed:
        return {"error": "no items parsed", "parse_errors": parse_errors}

    if num_orders < 1:
        num_orders = 1
    if num_orders > 1000:
        return {"error": "num_orders out of range (1..1000)"}

    req, unknown = resolve_names(sde_dir, parsed)
    if not req:
        return {"error": "no recognised item names",
                 "unknown": unknown, "parse_errors": parse_errors}

    # Multiply per requested order count.
    req_mult = {tid: qty * num_orders for tid, qty in req.items()}

    snap = latest_snapshot(data_dir)
    if snap is None:
        return {"error": "no snapshots yet"}
    snap_path = orders_path(data_dir, snap)

    sell_by_loc, buy_orders, loc_meta = _scan_snapshot(
        snap_path, set(req_mult))

    cache = get_cache(sde_dir)
    type_names = cache["type_names"]

    items_summary = []
    for tid, qty in req_mult.items():
        items_summary.append({
            "type_id": tid,
            "name": type_names.get(tid, str(tid)),
            "qty": qty,
        })

    if mode == "sell":
        cons_map = _cons_of_system(sde_dir, resolver)
        results = best_sell_locations(
            req_mult, buy_orders, loc_meta, resolver, cons_map, top_n=top_n)
    else:
        results = best_buy_locations(req_mult, sell_by_loc, top_n=top_n)

    # Annotate breakdown with type names.
    for r in results:
        for b in r["items"]:
            b["name"] = type_names.get(b["type_id"], str(b["type_id"]))

    return {
        "mode": mode,
        "num_orders": num_orders,
        "items": items_summary,
        "unknown": unknown,
        "parse_errors": parse_errors,
        "results": results,
        "snapshot_unix": snap,
    }


# ---------------------------------------------------------------------------
# Location-name resolution (ESI batch lookup with retry on failed IDs)
# ---------------------------------------------------------------------------
def resolve_location_names(loc_ids: Iterable[int]) -> dict[int, str]:
    """Resolve station/structure IDs to names via ESI /universe/names/.

    NPC stations resolve cleanly; player structures (>= 1e12) require auth and
    will fall back to ``"Structure {id}"`` on failure. Failed IDs are retried
    individually so one bad ID doesn't poison the whole batch.
    """
    ids = sorted({int(x) for x in loc_ids if x is not None})
    if not ids:
        return {}
    import requests
    out: dict[int, str] = {}

    def _try(batch: list[int]) -> bool:
        try:
            resp = requests.post(
                "https://esi.evetech.net/latest/universe/names/",
                json=batch,
                headers={"User-Agent": "eveMarket/1.0"},
                timeout=10,
            )
            if resp.status_code != 200:
                return False
            for item in resp.json():
                out[int(item["id"])] = item.get("name") or str(item["id"])
            return True
        except Exception:
            return False

    # Split NPC stations (resolvable) from structures (not without auth).
    npc = [i for i in ids if i < 1_000_000_000_000]
    structs = [i for i in ids if i >= 1_000_000_000_000]

    if npc and not _try(npc):
        # Retry one-by-one to skip any bad IDs.
        for i in npc:
            _try([i])

    for i in structs:
        out.setdefault(i, f"Structure {i}")
    for i in npc:
        out.setdefault(i, f"Station {i}")
    return out


# ---------------------------------------------------------------------------
# HTML page — served by GET /appraisal-maxxer
# ---------------------------------------------------------------------------
APPRAISAL_HTML = b"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Appraisal Maxxer</title>
<style>
:root{--bg:#0d1117;--bg2:#161b22;--bg3:#1f2428;--bd:#30363d;--tx:#c9d1d9;
      --hd:#e6edf3;--ac:#58a6ff;--green:#3fb950;--orange:#d29922;--red:#f85149}
*{box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;
     background:var(--bg);color:var(--tx);margin:0;padding:1.2rem 2rem;
     line-height:1.45;max-width:1100px}
h1{color:var(--hd);font-size:1.6rem;border-bottom:1px solid var(--bd);
   padding-bottom:.4rem;margin-top:0}
h2{color:var(--hd);font-size:1.1rem;margin-top:1.4rem}
a{color:var(--ac);text-decoration:none}a:hover{text-decoration:underline}
label{display:block;color:var(--hd);font-size:.85rem;margin:.6rem 0 .25rem}
textarea,input,select,button{font-family:inherit;font-size:.9rem;
     background:var(--bg3);color:var(--tx);border:1px solid var(--bd);
     border-radius:5px;padding:.5em .7em;outline:none}
textarea{width:100%;min-height:200px;resize:vertical;
         font-family:Consolas,"SFMono-Regular",monospace;font-size:.85rem}
textarea:focus,input:focus,select:focus{border-color:var(--ac)}
.row{display:flex;gap:.8rem;align-items:flex-end;flex-wrap:wrap;margin-top:.4rem}
.col{display:flex;flex-direction:column}
button{cursor:pointer;background:#21262d;color:var(--hd);font-weight:600;
       padding:.6em 1.1em;border:1px solid var(--bd);transition:background .1s}
button:hover{background:#30363d}
button.primary{background:#238636;border-color:#2ea043;color:#fff}
button.primary:hover{background:#2ea043}
button.danger{background:#8b2a2a;border-color:#a04040;color:#fff}
button.danger:hover{background:#a13838}
button:disabled{opacity:.5;cursor:not-allowed}
table{border-collapse:collapse;width:100%;margin:.6em 0;font-size:.88rem}
th,td{border:1px solid var(--bd);padding:.4em .65em;text-align:left}
th{background:var(--bg2);color:var(--hd)}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
.muted{color:#7d8590}
.err{color:var(--red)}.ok{color:var(--green)}.warn{color:var(--orange)}
.card{background:var(--bg2);border:1px solid var(--bd);border-radius:6px;
      padding:.8em 1em;margin:.6em 0}
.card h3{margin:.1em 0 .4em;color:var(--hd);font-size:1rem}
details{margin:.4em 0}
summary{cursor:pointer;color:var(--ac)}
.spinner{display:inline-block;width:14px;height:14px;border:2px solid var(--bd);
         border-top-color:var(--ac);border-radius:50%;
         animation:spin .7s linear infinite;vertical-align:-2px;margin-right:.4em}
@keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<h1>Appraisal Maxxer</h1>
<p class="muted">Paste a multibuy list (in-game format or <code>Name Qty</code> per line)
and find the single best station to either <strong>buy</strong> all items from sell orders
or <strong>sell</strong> all items into buy orders.</p>

<label for="paste">Items</label>
<textarea id="paste" placeholder="Transmitter\t60\nWater-Cooled CPU\t60\nNanites\t60\n\n...or...\n\nTransmitter 60\nWater-Cooled CPU 60"></textarea>

<div class="row">
  <div class="col">
    <label for="orders">Number of orders</label>
    <input id="orders" type="number" min="1" max="1000" value="1" style="width:7em">
  </div>
  <div class="col">
    <label for="topn">Top results</label>
    <input id="topn" type="number" min="1" max="50" value="10" style="width:7em">
  </div>
  <div class="col">
    <button id="btn-buy" class="primary" type="button">Where to buy</button>
  </div>
  <div class="col">
    <button id="btn-sell" class="danger" type="button">Where to sell</button>
  </div>
</div>

<div id="status" class="muted" style="margin-top:.8rem"></div>
<div id="out"></div>

<script>
const fmt = n => {
  if (n == null || isNaN(n)) return '';
  if (n >= 1e12) return (n/1e12).toFixed(2) + 'T';
  if (n >= 1e9)  return (n/1e9).toFixed(2)  + 'B';
  if (n >= 1e6)  return (n/1e6).toFixed(2)  + 'M';
  if (n >= 1e3)  return (n/1e3).toFixed(2)  + 'K';
  return n.toFixed(2);
};
const fmtIsk = n => fmt(n) + ' ISK';
const fmtInt = n => (n == null || isNaN(n)) ? '' : n.toLocaleString();

const $ = id => document.getElementById(id);
const setStatus = (html, cls='muted') => {
  $('status').className = cls;
  $('status').innerHTML = html;
};

async function run(mode){
  const text = $('paste').value;
  const orders = parseInt($('orders').value, 10) || 1;
  const topn   = parseInt($('topn').value, 10) || 10;
  if (!text.trim()) { setStatus('Paste some items first.', 'err'); return; }
  $('btn-buy').disabled = $('btn-sell').disabled = true;
  setStatus('<span class="spinner"></span>Computing...');
  $('out').innerHTML = '';
  try{
    const resp = await fetch('/appraisal-maxxer', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({items:text, num_orders:orders, mode, top_n:topn}),
    });
    const data = await resp.json();
    if (!resp.ok || data.error){
      setStatus('Error: ' + (data.error || resp.statusText), 'err');
      if (data.parse_errors && data.parse_errors.length){
        $('out').innerHTML = '<div class="card"><h3 class="err">Parse errors</h3><ul>' +
          data.parse_errors.map(e=>'<li>'+escapeHtml(e)+'</li>').join('') + '</ul></div>';
      }
      return;
    }
    render(data);
  } catch(e){
    setStatus('Network error: ' + e.message, 'err');
  } finally {
    $('btn-buy').disabled = $('btn-sell').disabled = false;
  }
}

function escapeHtml(s){
  return String(s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[c]));
}

function render(d){
  const verb = d.mode === 'sell' ? 'sell to' : 'buy from';
  const totalLabel = d.mode === 'sell' ? 'Proceeds' : 'Cost';
  let html = '';

  // Summary
  html += '<div class="card"><h3>Request</h3>';
  html += `<p class="muted">Snapshot unix: ${d.snapshot_unix} &middot; ${d.num_orders} order(s) &middot; mode: <strong>${d.mode}</strong></p>`;
  html += '<table><thead><tr><th>Item</th><th class="num">Total Qty</th></tr></thead><tbody>';
  for (const it of d.items){
    html += `<tr><td>${escapeHtml(it.name)}</td><td class="num">${fmtInt(it.qty)}</td></tr>`;
  }
  html += '</tbody></table>';
  if (d.unknown && d.unknown.length){
    html += '<p class="warn">Unknown items (skipped): ' +
      d.unknown.map(escapeHtml).join(', ') + '</p>';
  }
  if (d.parse_errors && d.parse_errors.length){
    html += '<p class="warn">Parse warnings:<br>' +
      d.parse_errors.map(escapeHtml).join('<br>') + '</p>';
  }
  html += '</div>';

  if (!d.results || !d.results.length){
    html += `<div class="card"><h3 class="err">No station can fully ${verb} all items</h3>` +
      '<p class="muted">Try fewer orders, fewer items, or check that buy/sell orders exist for these types.</p></div>';
    $('out').innerHTML = html;
    setStatus('Done.', 'muted');
    return;
  }

  setStatus(`<span class="ok">Best location: <strong>${escapeHtml(d.results[0].location_name || ('#' + d.results[0].location_id))}</strong> &mdash; ${totalLabel}: <strong>${fmtIsk(d.results[0].total)}</strong></span>`);

  html += `<h2>Top ${d.results.length} stations to ${verb}</h2>`;
  html += '<table><thead><tr><th>#</th><th>Station</th><th class="num">Total ' + totalLabel + '</th><th>Breakdown</th></tr></thead><tbody>';
  for (let i = 0; i < d.results.length; i++){
    const r = d.results[i];
    const name = r.location_name || ('#' + r.location_id);
    const detailRows = r.items.map(b => {
      const unit = d.mode === 'sell' ? b.top_unit_price : b.best_unit_price;
      const total = d.mode === 'sell' ? b.proceeds : b.cost;
      return `<tr><td>${escapeHtml(b.name)}</td><td class="num">${fmtInt(b.qty)}</td><td class="num">${fmtIsk(unit)}</td><td class="num">${fmtIsk(b.avg_unit_price)}</td><td class="num">${fmtIsk(total)}</td></tr>`;
    }).join('');
    html += `<tr><td class="num">${i+1}</td><td>${escapeHtml(name)} <span class="muted">#${r.location_id}</span></td><td class="num"><strong>${fmtIsk(r.total)}</strong></td><td><details><summary>${r.items.length} items</summary>` +
      '<table><thead><tr><th>Item</th><th class="num">Qty</th><th class="num">' +
      (d.mode === 'sell' ? 'Top price' : 'Best price') +
      '</th><th class="num">Avg unit</th><th class="num">' + totalLabel +
      '</th></tr></thead><tbody>' + detailRows + '</tbody></table></details></td></tr>';
  }
  html += '</tbody></table>';
  $('out').innerHTML = html;
}

$('btn-buy').addEventListener('click',  () => run('buy'));
$('btn-sell').addEventListener('click', () => run('sell'));
</script>
</body>
</html>"""

