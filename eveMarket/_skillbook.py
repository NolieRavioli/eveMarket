"""Skillbook-Maxxer: decide which skillbooks are cheaper to buy on the
market vs. the NPC seed price (with a configurable remote-injection markup).

Input
-----
A multibuy paste (in-game format or ``Name<sep>Qty`` lines), restricted to
items that are actually skillbooks (categoryID 16 / groupID 255).  A single
trade hub is selected.

Logic
-----
For each skillbook in the paste, look at the cheapest sell order *at the
selected trade hub station*.  Define::

    threshold = base_price * markup       (markup defaults to 1.2)

If ``best_sell_price <= threshold``: buy it on the market (cheaper than the
remote-inject NPC charge).  Otherwise: skip - inject via NPC store.

Output
------
A list of skillbooks (with their qty) for which the market is cheaper than
NPC, formatted as a multibuy-friendly ``Name\\tQty`` block ready to paste
back into EVE's multibuy window.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Optional

from ._appraisal import parse_paste, resolve_names
from .index import iter_snapshot
from .snapshot import latest_snapshot, orders_path


# ---------------------------------------------------------------------------
# The five canonical trade hub stations
# ---------------------------------------------------------------------------
TRADE_HUBS: dict[str, dict] = {
    "jita": {
        "station_id":   60003760,
        "system_id":    30000142,
        "region_id":    10000002,
        "label":        "Jita IV - Moon 4 - Caldari Navy Assembly Plant",
    },
    "amarr": {
        "station_id":   60008494,
        "system_id":    30002187,
        "region_id":    10000043,
        "label":        "Amarr VIII (Oris) - Emperor Family Academy",
    },
    "dodixie": {
        "station_id":   60011866,
        "system_id":    30002659,
        "region_id":    10000032,
        "label":        "Dodixie IX - Moon 20 - Federation Navy Assembly Plant",
    },
    "rens": {
        "station_id":   60004588,
        "system_id":    30002510,
        "region_id":    10000030,
        "label":        "Rens VI - Moon 8 - Brutor Tribe Treasury",
    },
    "hek": {
        "station_id":   60005686,
        "system_id":    30002053,
        "region_id":    10000042,
        "label":        "Hek VIII - Moon 12 - Boundless Creation Factory",
    },
}

DEFAULT_MARKUP = 1.2


# ---------------------------------------------------------------------------
# Skillbook base-price index (built lazily from types.jsonl)
# ---------------------------------------------------------------------------
_LOCK = threading.Lock()
_SKILL_BASE: dict[int, float] | None = None   # type_id -> NPC basePrice


def _load_skill_base(sde_dir: Path) -> dict[int, float]:
    """Return {type_id: basePrice} for every type in categoryID 16 (Skill)."""
    global _SKILL_BASE
    with _LOCK:
        if _SKILL_BASE is not None:
            return _SKILL_BASE

        # First find all skill groupIDs (groups whose categoryID == 16).
        skill_groups: set[int] = set()
        gpath = sde_dir / "groups.jsonl"
        if gpath.exists():
            with gpath.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if int(row.get("categoryID") or 0) == 16:
                        skill_groups.add(int(row["_key"]))

        out: dict[int, float] = {}
        tpath = sde_dir / "types.jsonl"
        if tpath.exists():
            with tpath.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    gid = row.get("groupID")
                    if gid is None or int(gid) not in skill_groups:
                        continue
                    bp = row.get("basePrice")
                    if bp is None:
                        continue
                    out[int(row["_key"])] = float(bp)
        _SKILL_BASE = out
        return _SKILL_BASE


# ---------------------------------------------------------------------------
# Snapshot scan: cheapest sell offer per type at the chosen station
# ---------------------------------------------------------------------------
def _best_sell_at_station(snap_path: Path, station_id: int,
                          type_ids: set[int]) -> dict[int, dict]:
    """Return {type_id: {"price": .., "volume_remain": ..}} for the cheapest
    sell order at *station_id* for each requested type.  Missing types are
    absent from the returned dict."""
    best: dict[int, dict] = {}
    for o in iter_snapshot(snap_path):
        if o.get("is_buy_order"):
            continue
        loc = o.get("location_id")
        if loc is None or int(loc) != station_id:
            continue
        tid = o.get("type_id")
        if tid is None:
            continue
        tid = int(tid)
        if tid not in type_ids:
            continue
        price = float(o.get("price") or 0.0)
        vol = int(o.get("volume_remain") or 0)
        if vol <= 0 or price <= 0:
            continue
        cur = best.get(tid)
        if cur is None or price < cur["price"]:
            best[tid] = {"price": price, "volume_remain": vol}
    return best


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
def skillbook_maxx(sde_dir: Path, data_dir: Path,
                   text: str, hub_key: str,
                   markup: float = DEFAULT_MARKUP) -> dict:
    """Top-level entry called by the HTTP route."""
    hub = TRADE_HUBS.get((hub_key or "").lower())
    if hub is None:
        return {"error": f"unknown hub {hub_key!r}; valid: "
                + ", ".join(sorted(TRADE_HUBS))}

    if markup <= 0:
        return {"error": "markup must be > 0"}

    parsed, parse_errors = parse_paste(text)
    if not parsed:
        return {"error": "no items parsed", "parse_errors": parse_errors}

    req, unknown = resolve_names(sde_dir, parsed)
    if not req:
        return {"error": "no recognised item names",
                "unknown": unknown, "parse_errors": parse_errors}

    skill_base = _load_skill_base(sde_dir)

    # Filter requested items down to actual skillbooks.
    from .sde_market import get_cache
    type_names = get_cache(sde_dir)["type_names"]

    skill_req: dict[int, int] = {}
    not_skill: list[str] = []
    no_base:   list[str] = []
    for tid, qty in req.items():
        if tid not in skill_base:
            # Not a skill, or a skill without basePrice.
            name = type_names.get(tid, str(tid))
            # Decide which bucket: we know it's not a skillbook if its group
            # category isn't 16; we differentiate using the lookup we built.
            if tid in skill_base:
                no_base.append(name)
            else:
                not_skill.append(name)
            continue
        skill_req[tid] = qty

    if not skill_req:
        return {
            "error": "no skillbooks in paste (or none have an SDE base price)",
            "not_skill": not_skill, "no_base_price": no_base,
            "parse_errors": parse_errors, "unknown": unknown,
        }

    snap = latest_snapshot(data_dir)
    if snap is None:
        return {"error": "no snapshots yet"}
    snap_path = orders_path(data_dir, snap)

    best = _best_sell_at_station(snap_path, hub["station_id"],
                                 set(skill_req))

    buy_rows:  list[dict] = []
    skip_rows: list[dict] = []
    missing_rows: list[dict] = []  # not for sale at this hub at all

    for tid, qty in skill_req.items():
        name = type_names.get(tid, str(tid))
        base = skill_base[tid]
        threshold = base * markup
        offer = best.get(tid)
        row = {
            "type_id":   tid,
            "name":      name,
            "qty":       qty,
            "base_price": base,
            "threshold": threshold,
        }
        if offer is None:
            row["sell_price"] = None
            row["volume_remain"] = 0
            missing_rows.append(row)
            continue
        row["sell_price"] = offer["price"]
        row["volume_remain"] = offer["volume_remain"]
        row["fillable_qty"] = min(qty, offer["volume_remain"])
        if offer["price"] <= threshold:
            row["savings_per_unit"] = threshold - offer["price"]
            row["total_savings"] = row["savings_per_unit"] * row["fillable_qty"]
            buy_rows.append(row)
        else:
            row["overpay_per_unit"] = offer["price"] - threshold
            skip_rows.append(row)

    # Build the multibuy paste block from buy_rows (in original paste order
    # if possible).
    multibuy_lines = [
        f"{r['name']}\t{r['fillable_qty']}"
        for r in buy_rows if r["fillable_qty"] > 0
    ]
    multibuy = "\n".join(multibuy_lines)

    total_market_cost = sum(
        r["sell_price"] * r["fillable_qty"] for r in buy_rows
    )
    total_npc_cost_if_market_buys = sum(
        r["base_price"] * markup * r["fillable_qty"] for r in buy_rows
    )
    total_savings = total_npc_cost_if_market_buys - total_market_cost

    return {
        "hub": {
            "key":        hub_key.lower(),
            "label":      hub["label"],
            "station_id": hub["station_id"],
            "system_id":  hub["system_id"],
            "region_id":  hub["region_id"],
        },
        "markup":      markup,
        "snapshot_unix": snap,
        "buy":         buy_rows,
        "skip":        skip_rows,
        "missing":     missing_rows,
        "not_skill":   not_skill,
        "no_base_price": no_base,
        "unknown":     unknown,
        "parse_errors": parse_errors,
        "multibuy":    multibuy,
        "totals": {
            "market_cost":      total_market_cost,
            "npc_equivalent":   total_npc_cost_if_market_buys,
            "savings":          total_savings,
            "buy_lines":        len(buy_rows),
            "skip_lines":       len(skip_rows),
            "missing_lines":    len(missing_rows),
        },
    }


# ---------------------------------------------------------------------------
# HTML page -- served by GET /skillbook-maxxer
# ---------------------------------------------------------------------------
SKILLBOOK_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Skillbook Maxxer</title>
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
textarea:focus,input:focus{border-color:var(--ac)}
.row{display:flex;gap:.8rem;align-items:flex-end;flex-wrap:wrap;margin-top:.4rem}
.col{display:flex;flex-direction:column}
button{cursor:pointer;background:#21262d;color:var(--hd);font-weight:600;
       padding:.6em 1.1em;border:1px solid var(--bd);transition:background .1s}
button:hover{background:#30363d}
button.primary{background:#238636;border-color:#2ea043;color:#fff}
button.primary:hover{background:#2ea043}
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
.spinner{display:inline-block;width:14px;height:14px;border:2px solid var(--bd);
         border-top-color:var(--ac);border-radius:50%;
         animation:spin .7s linear infinite;vertical-align:-2px;margin-right:.4em}
@keyframes spin{to{transform:rotate(360deg)}}

/* hub radio bubbles */
.hubs{display:flex;flex-wrap:wrap;gap:.4rem;margin:.3rem 0 .2rem}
.hubs input{position:absolute;opacity:0;pointer-events:none}
.hubs label{display:inline-block;margin:0;padding:.45em 1em;border:1px solid var(--bd);
            border-radius:999px;background:var(--bg3);color:var(--tx);cursor:pointer;
            font-size:.85rem;transition:all .12s}
.hubs label:hover{background:#262c33;color:var(--hd)}
.hubs input:checked + label{background:#1f6feb;border-color:#388bfd;color:#fff}

.copybox{display:flex;gap:.4rem;align-items:flex-start;margin-top:.4rem}
.copybox textarea{min-height:120px;flex:1;background:#0b1117}
.copybtn{align-self:flex-start;padding:.5em 1em}
</style>
</head>
<body>
<h1>Skillbook Maxxer</h1>
<p class="muted">Paste a multibuy list of skillbooks. The tool keeps only the ones where
the cheapest sell order at the selected trade hub is <strong>at most
<code>base_price &times; markup</code></strong> (default 1.2&times;, the rough
remote-injection NPC surcharge), and outputs a fresh multibuy paste of just
those.</p>

<label>Trade hub (single-select)</label>
<div class="hubs" id="hubs">
  <input type="radio" id="hub-jita"    name="hub" value="jita"    checked><label for="hub-jita">Jita</label>
  <input type="radio" id="hub-amarr"   name="hub" value="amarr"           ><label for="hub-amarr">Amarr</label>
  <input type="radio" id="hub-dodixie" name="hub" value="dodixie"         ><label for="hub-dodixie">Dodixie</label>
  <input type="radio" id="hub-rens"    name="hub" value="rens"            ><label for="hub-rens">Rens</label>
  <input type="radio" id="hub-hek"     name="hub" value="hek"             ><label for="hub-hek">Hek</label>
</div>

<label for="paste">Skillbooks</label>
<textarea id="paste" placeholder="Caldari Frigate\t1\nGunnery\t1\nDrones\t1\n...one skill per line, EVE-multibuy format or 'Name Qty' also fine"></textarea>

<div class="row">
  <div class="col">
    <label for="markup">NPC markup multiplier</label>
    <input id="markup" type="number" min="0.1" step="0.01" value="1.20" style="width:7em">
  </div>
  <div class="col">
    <button id="btn-run" class="primary" type="button">Compute multibuy</button>
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
const setStatus = (html, cls='muted') => { $('status').className = cls; $('status').innerHTML = html; };

function escapeHtml(s){
  return String(s).replace(/[&<>"']/g, c => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  }[c]));
}

async function run(){
  const text = $('paste').value;
  const hub  = document.querySelector('input[name=hub]:checked').value;
  const markup = parseFloat($('markup').value) || 1.2;
  if (!text.trim()) { setStatus('Paste some skillbooks first.', 'err'); return; }
  $('btn-run').disabled = true;
  setStatus('<span class="spinner"></span>Computing...');
  $('out').innerHTML = '';
  try{
    const resp = await fetch('/skillbook-maxxer', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({items:text, hub:hub, markup:markup}),
    });
    const data = await resp.json();
    if (!resp.ok || data.error){
      setStatus('Error: ' + (data.error || resp.statusText), 'err');
      return;
    }
    render(data);
  } catch(e){
    setStatus('Network error: ' + e.message, 'err');
  } finally {
    $('btn-run').disabled = false;
  }
}

function render(d){
  let html = '';

  // Summary card with multibuy paste
  html += '<div class="card"><h3>Multibuy (paste back into EVE)</h3>';
  html += `<p class="muted">Hub: <strong>${escapeHtml(d.hub.label)}</strong> &middot; markup &times;${d.markup.toFixed(2)} &middot; snapshot ${d.snapshot_unix}</p>`;
  if (!d.multibuy){
    html += '<p class="warn">No skillbooks were cheaper than the NPC threshold at this hub.</p>';
  } else {
    html += '<div class="copybox">';
    html += '<textarea id="multibuy" readonly>' + escapeHtml(d.multibuy) + '</textarea>';
    html += '<button class="copybtn" id="copy-mb" type="button">Copy</button>';
    html += '</div>';
    const t = d.totals;
    html += `<p class="ok" style="margin-top:.6em">Buy <strong>${t.buy_lines}</strong> skills at the market for <strong>${fmtIsk(t.market_cost)}</strong>` +
            ` &mdash; NPC-equivalent would have cost ${fmtIsk(t.npc_equivalent)} (savings <strong>${fmtIsk(t.savings)}</strong>).</p>`;
  }
  html += '</div>';

  // Detail tables
  const rowTpl = (r, kind) => {
    const sell = r.sell_price == null ? '<span class="muted">none</span>' : fmtIsk(r.sell_price);
    const cls  = kind === 'buy' ? 'ok' : (kind === 'skip' ? 'warn' : 'err');
    return `<tr class="${cls}"><td>${escapeHtml(r.name)}</td>` +
           `<td class="num">${fmtInt(r.qty)}</td>` +
           `<td class="num">${fmtIsk(r.base_price)}</td>` +
           `<td class="num">${fmtIsk(r.threshold)}</td>` +
           `<td class="num">${sell}</td>` +
           `<td class="num">${fmtInt(r.volume_remain)}</td></tr>`;
  };

  if (d.buy.length){
    html += '<h2 class="ok">BUY at market (sell price ≤ threshold)</h2>';
    html += '<table><thead><tr><th>Skill</th><th class="num">Qty</th><th class="num">NPC base</th><th class="num">Threshold</th><th class="num">Best sell</th><th class="num">Avail</th></tr></thead><tbody>';
    html += d.buy.map(r => rowTpl(r, 'buy')).join('');
    html += '</tbody></table>';
  }
  if (d.skip.length){
    html += '<h2 class="warn">SKIP (market more expensive than NPC injection)</h2>';
    html += '<table><thead><tr><th>Skill</th><th class="num">Qty</th><th class="num">NPC base</th><th class="num">Threshold</th><th class="num">Best sell</th><th class="num">Avail</th></tr></thead><tbody>';
    html += d.skip.map(r => rowTpl(r, 'skip')).join('');
    html += '</tbody></table>';
  }
  if (d.missing.length){
    html += '<h2 class="err">NOT FOR SALE at this hub</h2>';
    html += '<table><thead><tr><th>Skill</th><th class="num">Qty</th><th class="num">NPC base</th><th class="num">Threshold</th><th class="num">Best sell</th><th class="num">Avail</th></tr></thead><tbody>';
    html += d.missing.map(r => rowTpl(r, 'missing')).join('');
    html += '</tbody></table>';
  }

  const warns = [];
  if (d.not_skill && d.not_skill.length){
    warns.push('Not skillbooks (skipped): ' + d.not_skill.map(escapeHtml).join(', '));
  }
  if (d.no_base_price && d.no_base_price.length){
    warns.push('No NPC base price (skipped): ' + d.no_base_price.map(escapeHtml).join(', '));
  }
  if (d.unknown && d.unknown.length){
    warns.push('Unknown names (skipped): ' + d.unknown.map(escapeHtml).join(', '));
  }
  if (d.parse_errors && d.parse_errors.length){
    warns.push('Parse warnings: ' + d.parse_errors.map(escapeHtml).join(' | '));
  }
  if (warns.length){
    html += '<div class="card"><h3 class="warn">Warnings</h3><p>' + warns.join('<br>') + '</p></div>';
  }

  $('out').innerHTML = html;
  setStatus('Done.', 'muted');

  const copy = $('copy-mb');
  if (copy){
    copy.addEventListener('click', async () => {
      try{
        await navigator.clipboard.writeText(d.multibuy);
        copy.textContent = 'Copied!';
        setTimeout(() => copy.textContent = 'Copy', 1200);
      } catch(e){
        const ta = $('multibuy'); ta.select(); document.execCommand('copy');
        copy.textContent = 'Copied!';
        setTimeout(() => copy.textContent = 'Copy', 1200);
      }
    });
  }
}

$('btn-run').addEventListener('click', run);
</script>
</body>
</html>""".encode("utf-8")
