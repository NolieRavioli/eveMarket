#!/usr/bin/env python3
"""Manual endpoint tester for eveMarket.

Usage:
    python testing/test_endpoints.py [--host HOST] [--port PORT]

Requires the server to be running:
    python eveMarket.py

Tests:
  1. POST /history/{location_id}/1h  -- The Forge region (10000002)
  2. POST /history/{location_id}/1h  -- Jita 4-4 station (60003760)
  3. POST /stats/{location_id}       -- The Forge region
  4. POST /stats/{location_id}       -- Jita 4-4 station (strict)
  5. POST /stats/{location_id}?attribution=jump_weighted  -- Jita 4-4 station
  6. GET  /inferred/{location_id}    -- Jita 4-4 station (strict)
  7. GET  /inferred/{location_id}?attribution=jump_weighted -- Jita 4-4 station
  8. GET  /orders                    -- full universe download (timing test)
"""
from __future__ import annotations

import argparse
import json
import sys
import textwrap
import time
from typing import Any

import urllib.request
import urllib.error

# ── well-known IDs ─────────────────────────────────────────────────────────────
THE_FORGE_REGION  = 10_000_002
JITA_44_STATION   = 60_003_760      # Jita IV - Moon 4 - Caldari Navy Assembly Plant
TEST_TYPE_IDS     = [34, 35, 36, 37, 38]  # Trit, Pyerite, Mex, Isogen, Nocxium

# ── helpers ────────────────────────────────────────────────────────────────────
def _base(host: str, port: int) -> str:
    return f"http://{host}:{port}"


def _header(title: str) -> None:
    print()
    bar = "─" * (len(title) + 4)
    print(f"┌{bar}┐")
    print(f"│  {title}  │")
    print(f"└{bar}┘")


def _ok(label: str, value: Any = None) -> None:
    suffix = f"  {value}" if value is not None else ""
    print(f"  ✓  {label}{suffix}")


def _info(label: str, value: Any) -> None:
    print(f"     {label}: {value}")


def _truncate(obj: Any, max_keys: int = 3) -> str:
    """Pretty-print a dict/list but truncate after max_keys entries."""
    if isinstance(obj, dict):
        keys = list(obj.keys())
        shown = {k: obj[k] for k in keys[:max_keys]}
        extra = len(keys) - max_keys
        s = json.dumps(shown, indent=2)
        if extra > 0:
            s += f"\n  ... and {extra} more type(s)"
        return s
    if isinstance(obj, list):
        return json.dumps(obj[:max_keys]) + (f" … (+{len(obj)-max_keys})" if len(obj) > max_keys else "")
    return str(obj)


def post_json(url: str, payload: list) -> tuple[int, Any, float]:
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
        method="POST",
    )
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            elapsed = time.monotonic() - t0
            return resp.status, json.loads(resp.read()), elapsed
    except urllib.error.HTTPError as e:
        elapsed = time.monotonic() - t0
        try:
            body_text = json.loads(e.read())
        except Exception:
            body_text = e.reason
        return e.code, body_text, elapsed


def get_json(url: str) -> tuple[int, Any, float]:
    req = urllib.request.Request(url, method="GET")
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            elapsed = time.monotonic() - t0
            return resp.status, json.loads(resp.read()), elapsed
    except urllib.error.HTTPError as e:
        elapsed = time.monotonic() - t0
        try:
            body_text = json.loads(e.read())
        except Exception:
            body_text = e.reason
        return e.code, body_text, elapsed


def stream_ndjson(url: str) -> tuple[int, int, float, dict[str, str]]:
    """Stream NDJSON, return (http_status, row_count, elapsed_s, headers)."""
    req = urllib.request.Request(url, method="GET")
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            headers = dict(resp.headers)
            count = 0
            for raw_line in resp:
                line = raw_line.strip()
                if line:
                    count += 1
                    if count % 100_000 == 0:
                        elapsed = time.monotonic() - t0
                        print(f"     … {count:,} rows  ({elapsed:.1f}s)", end="\r", flush=True)
            elapsed = time.monotonic() - t0
            return resp.status, count, elapsed, headers
    except urllib.error.HTTPError as e:
        return e.code, 0, time.monotonic() - t0, {}


# ── tests ──────────────────────────────────────────────────────────────────────

def test_history(base: str) -> None:
    _header("POST /history — 1h range")

    cases = [
        ("The Forge (region)", f"{base}/history/{THE_FORGE_REGION}/1h"),
        ("Jita 4-4  (station)", f"{base}/history/{JITA_44_STATION}/1h"),
    ]
    for label, url in cases:
        code, data, elapsed = post_json(url, TEST_TYPE_IDS)
        if code == 200:
            _ok(label, f"HTTP {code}  ({elapsed:.2f}s)")
            for tid, rec in list(data.items())[:3]:
                if rec:
                    _info(f"  type {tid}", f"avg={rec.get('average')} vol={rec.get('volume')} date={rec.get('date')} range={rec.get('range')}s")
                else:
                    _info(f"  type {tid}", "null (no history)")
        else:
            _ok(label, f"HTTP {code}  ({elapsed:.2f}s)")
            _info("  response", data)


def test_stats(base: str) -> None:
    _header("POST /stats — region and station (strict + jump-weighted)")

    cases = [
        ("The Forge (region)",                          f"{base}/stats/{THE_FORGE_REGION}"),
        ("Jita 4-4  (station, strict)",                 f"{base}/stats/{JITA_44_STATION}"),
        ("Jita 4-4  (station, attribution=jump_weighted)", f"{base}/stats/{JITA_44_STATION}?attribution=jump_weighted"),
    ]
    for label, url in cases:
        code, data, elapsed = post_json(url, TEST_TYPE_IDS)
        if code == 200:
            _ok(label, f"HTTP {code}  ({elapsed:.2f}s)")
            for tid, rec in list(data.items())[:2]:
                buy  = rec.get("buy", {})
                sell = rec.get("sell", {})
                attr = rec.get("attribution")
                _info(f"  type {tid} buy",
                      f"vol={buy.get('volume')} orders={buy.get('orderCount')}"
                      + (f" exact={buy.get('exact_volume')} est={buy.get('estimated_volume')}" if attr else "")
                      + (f" [attr={attr}]" if attr else ""))
                _info(f"  type {tid} sell",
                      f"vol={sell.get('volume')} orders={sell.get('orderCount')}")
        else:
            _ok(label, f"HTTP {code}  ({elapsed:.2f}s)")
            _info("  response", data)


def test_inferred(base: str) -> None:
    _header("GET /inferred — station strict + jump-weighted")

    cases = [
        ("Jita 4-4  strict",                    f"{base}/inferred/{JITA_44_STATION}"),
        ("Jita 4-4  attribution=jump_weighted",  f"{base}/inferred/{JITA_44_STATION}?attribution=jump_weighted"),
    ]
    for label, url in cases:
        status, count, elapsed, headers = stream_ndjson(url)
        if status == 200:
            _ok(label, f"HTTP {status}  {count:,} trades  ({elapsed:.2f}s)")
            snap = headers.get("X-Snapshot-Unix") or headers.get("x-snapshot-unix")
            attr = headers.get("X-Attribution") or headers.get("x-attribution")
            if snap:
                _info("  X-Snapshot-Unix", snap)
            if attr:
                _info("  X-Attribution", attr)
        elif status == 503:
            _ok(label, f"HTTP {status}  (no snapshots yet — run server for a full cycle first)")
        else:
            _ok(label, f"HTTP {status}  ({elapsed:.2f}s)")


def test_orders_download(base: str) -> None:
    _header("GET /orders — full universe download (timing)")

    url = f"{base}/orders"
    print(f"  Streaming {url} …")
    status, count, elapsed, headers = stream_ndjson(url)
    # Clear progress line
    print(" " * 60, end="\r")
    if status == 200:
        _ok("Universe orders", f"HTTP {status}")
        snap = headers.get("X-Snapshot-Unix") or headers.get("x-snapshot-unix")
        _info("  rows streamed", f"{count:,}")
        _info("  elapsed      ", f"{elapsed:.2f}s")
        _info("  throughput   ", f"{count / elapsed:,.0f} rows/s" if elapsed > 0 else "n/a")
        if snap:
            _info("  X-Snapshot-Unix", snap)
    elif status == 503:
        _ok("Universe orders", f"HTTP {status}  (no snapshots yet)")
    else:
        _ok("Universe orders", f"HTTP {status}  ({elapsed:.2f}s)")


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="eveMarket endpoint tester")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=13307)
    args = ap.parse_args()

    base = _base(args.host, args.port)
    print(f"\nTarget: {base}")

    # Quick liveness check.
    try:
        code, data, _ = get_json(f"{base}/health")
        _ok("Health check", f"HTTP {code}  snapshots={data.get('snapshots')}  latest={data.get('latest_unix')}")
    except Exception as exc:
        print(f"\n  ✗  Cannot reach {base}: {exc}")
        print("     Start the server first:  python eveMarket.py")
        sys.exit(1)

    test_history(base)
    test_stats(base)
    test_inferred(base)
    test_orders_download(base)

    print()


if __name__ == "__main__":
    main()
