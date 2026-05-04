"""SDE market-browser data: market groups, types, regions, and stations.

Loads the relevant SDE .jsonl files once and exposes compact dicts for
the /browser API endpoints.  All data is cached in module-level variables
on first access (lazy) so we don't block server startup.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Optional

_LOCK = threading.Lock()
_CACHE: dict = {}


# ---------------------------------------------------------------------------
# Internal loaders
# ---------------------------------------------------------------------------

def _load_jsonl(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return rows


def _ensure_loaded(sde_dir: Path) -> dict:
    global _CACHE
    with _LOCK:
        if _CACHE:
            return _CACHE
        _CACHE = _build_cache(Path(sde_dir))
        return _CACHE


def _build_cache(sde_dir: Path) -> dict:
    # ---- market groups ----
    mg_rows = _load_jsonl(sde_dir / "marketGroups.jsonl")
    groups: dict[int, dict] = {}
    for row in mg_rows:
        gid = row.get("_key")
        if gid is None:
            continue
        gid = int(gid)
        name = (row.get("name") or {}).get("en") or str(gid)
        desc = (row.get("description") or {}).get("en") or ""
        icon_id = row.get("iconID")
        parent = row.get("parentGroupID")
        has_types = row.get("hasTypes", False)
        groups[gid] = {
            "id": gid,
            "name": name,
            "desc": desc,
            "icon": icon_id,
            "parent": int(parent) if parent is not None else None,
            "hasTypes": has_types,
            "children": [],
            "types": [],
        }

    # ---- types (market-listed only) ----
    # types.jsonl can be large; we stream it
    types: dict[int, dict] = {}
    types_path = sde_dir / "types.jsonl"
    if types_path.exists():
        with types_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                mgid = row.get("marketGroupID")
                if mgid is None:
                    continue
                tid = row.get("_key")
                if tid is None:
                    continue
                tid = int(tid)
                mgid = int(mgid)
                name = (row.get("name") or {}).get("en") or str(tid)
                desc = (row.get("description") or {}).get("en") or ""
                icon_id = row.get("iconID")
                types[tid] = {
                    "id": tid,
                    "name": name,
                    "desc": desc,
                    "icon": icon_id,
                    "group": mgid,
                }
                if mgid in groups:
                    groups[mgid]["types"].append(tid)

    # Build child relationships
    roots: list[int] = []
    for gid, g in groups.items():
        p = g["parent"]
        if p is not None and p in groups:
            groups[p]["children"].append(gid)
        else:
            roots.append(gid)

    # Sort children and roots alphabetically for stable display
    for g in groups.values():
        g["children"].sort(key=lambda x: groups[x]["name"])
        g["types"].sort()
    roots.sort(key=lambda x: groups[x]["name"])

    # ---- regions ----
    region_rows = _load_jsonl(sde_dir / "mapRegions.jsonl")
    regions: list[dict] = []
    for row in region_rows:
        rid = row.get("_key")
        if rid is None:
            continue
        rid = int(rid)
        name = (row.get("name") or {}).get("en") or str(rid)
        regions.append({"id": rid, "name": name})
    regions.sort(key=lambda r: r["name"])

    # ---- NPC stations ----
    # We need station names for the history dropdown.
    # npcStations.jsonl has solarSystemID + stationID + name (via stationOperations/useOperationName)
    # The name is constructed by the game from the operation; we just use the raw key.
    # stationID = _key, solarSystemID present.
    # For browser purposes we want (stationID, stationName) pairs.
    # The name is not stored directly; stations are identified by their numeric ID.
    # We gather them from the orders snapshot at runtime (too expensive here).
    # We return an empty list; the browser will populate stations from order data.
    stations: list[dict] = []
    st_rows = _load_jsonl(sde_dir / "npcStations.jsonl")
    sys_to_region: dict[int, int] = {}
    system_security: dict[int, float] = {}
    # Single pass over mapSolarSystems.jsonl for sys->region and security status
    sys_path = sde_dir / "mapSolarSystems.jsonl"
    if sys_path.exists():
        with sys_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row2 = json.loads(line)
                except json.JSONDecodeError:
                    continue
                sid = row2.get("_key") or row2.get("solarSystemID")
                rid2 = row2.get("regionID")
                if sid and rid2:
                    sys_to_region[int(sid)] = int(rid2)
                sec = row2.get("securityStatus")
                if sid is not None and sec is not None:
                    system_security[int(sid)] = float(sec)
    for row in st_rows:
        sid2 = row.get("_key")
        sys_id = row.get("solarSystemID")
        if sid2 is None:
            continue
        sid2 = int(sid2)
        region_id = sys_to_region.get(int(sys_id)) if sys_id else None
        stations.append({"id": sid2, "sys": int(sys_id) if sys_id else None, "region": region_id})

    # Build type_id -> name lookup used by order-list endpoint
    type_names: dict[int, str] = {tid: t["name"] for tid, t in types.items()}

    # Build region_id -> name map for fast lookup
    region_names: dict[int, str] = {r["id"]: r["name"] for r in regions}

    return {
        "groups": groups,
        "roots": roots,
        "types": types,
        "type_names": type_names,
        "regions": regions,
        "region_names": region_names,
        "stations": stations,
        "system_security": system_security,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_cache(sde_dir: Path) -> dict:
    return _ensure_loaded(sde_dir)


def market_group_tree(sde_dir: Path) -> list[dict]:
    """Return the root-level market groups as a compact tree (no types list)."""
    cache = get_cache(sde_dir)
    groups = cache["groups"]

    def _node(gid: int) -> dict:
        g = groups[gid]
        return {
            "id": gid,
            "name": g["name"],
            "desc": g["desc"],
            "icon": g["icon"],
            "hasTypes": g["hasTypes"],
            "children": [_node(c) for c in g["children"]],
        }

    return [_node(r) for r in cache["roots"]]


def types_in_group(sde_dir: Path, group_id: int) -> list[dict]:
    """Return types directly in this market group (leaf only)."""
    cache = get_cache(sde_dir)
    groups = cache["groups"]
    types = cache["types"]
    g = groups.get(group_id)
    if g is None:
        return []
    return [types[tid] for tid in g["types"] if tid in types]


def regions_list(sde_dir: Path) -> list[dict]:
    return get_cache(sde_dir)["regions"]


def type_name(sde_dir: Path, type_id: int) -> Optional[str]:
    return get_cache(sde_dir)["type_names"].get(type_id)


def type_info(sde_dir: Path, type_id: int) -> Optional[dict]:
    return get_cache(sde_dir)["types"].get(type_id)


def system_security(sde_dir: Path, system_id: int) -> Optional[float]:
    return get_cache(sde_dir)["system_security"].get(system_id)


def region_name(sde_dir: Path, region_id: int) -> Optional[str]:
    return get_cache(sde_dir)["region_names"].get(region_id)
