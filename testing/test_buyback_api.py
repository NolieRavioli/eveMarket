"""Unit tests for the buyback-api additions: /quote book walk, /esiHistory cache, /contracts/itemExchange.

    python -m unittest discover -s testing -p "test_buyback_api.py" -v

Self-contained: a tiny fake SDE + snapshot in a temp dir, a fake ESI client, the real HTTP server on a free port.
"""
from __future__ import annotations

import gzip
import json
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from eveMarket.esi_history import EsiHistoryStore, last_rollover, summarize  # noqa: E402
from eveMarket.jumps import JumpGraph  # noqa: E402
from eveMarket.location import LocationResolver  # noqa: E402
from eveMarket.quote import BookIndex, Order, build_book, parse_lines, QuoteError, quote, walk  # noqa: E402

FORGE, OTHER = 10000002, 10000020
JITA, PERIMETER, NEXT_DOOR = 30000142, 30000144, 30000145
JITA44, JITA_OTHER, PERIM_STATION, FAR_STATION = 60003760, 60003761, 60003762, 60003763
TRIT = 34


def write_jsonl(path: Path, rows) -> None:
    path.write_text("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows), encoding="utf-8")


def fake_sde(d: Path) -> Path:
    d.mkdir(parents=True, exist_ok=True)
    write_jsonl(d / "mapSolarSystems.jsonl", [
        {"_key": JITA, "regionID": FORGE, "constellationID": 20000020},
        {"_key": PERIMETER, "regionID": FORGE, "constellationID": 20000020},
        {"_key": NEXT_DOOR, "regionID": FORGE, "constellationID": 20000021},
        {"_key": 30000999, "regionID": OTHER, "constellationID": 20000999},
    ])
    write_jsonl(d / "mapConstellations.jsonl", [
        {"_key": 20000020, "regionID": FORGE, "solarSystemIDs": [JITA, PERIMETER]},
        {"_key": 20000021, "regionID": FORGE, "solarSystemIDs": [NEXT_DOOR]},
        {"_key": 20000999, "regionID": OTHER, "solarSystemIDs": [30000999]},
    ])
    write_jsonl(d / "npcStations.jsonl", [
        {"_key": JITA44, "solarSystemID": JITA}, {"_key": JITA_OTHER, "solarSystemID": JITA},
        {"_key": PERIM_STATION, "solarSystemID": PERIMETER}, {"_key": FAR_STATION, "solarSystemID": NEXT_DOOR},
    ])
    # Jita - Perimeter - NextDoor (NextDoor is 2 jumps from Jita)
    write_jsonl(d / "mapStargates.jsonl", [
        {"_key": 1, "solarSystemID": JITA, "destination": {"solarSystemID": PERIMETER}},
        {"_key": 2, "solarSystemID": PERIMETER, "destination": {"solarSystemID": NEXT_DOOR}},
    ])
    return d


def order(oid, *, buy, price, vol, loc, system, region=FORGE, rng="region", min_volume=1, tid=TRIT):
    return {"duration": 90, "is_buy_order": buy, "issued": "2026-11-01T00:00:00Z", "location_id": loc,
            "min_volume": min_volume, "order_id": oid, "price": price, "range": rng, "system_id": system,
            "type_id": tid, "volume_remain": vol, "volume_total": vol, "region_id": region}


SNAPSHOT = [
    # asks at Jita 4-4
    order(1, buy=False, price=4.10, vol=100, loc=JITA44, system=JITA),
    order(2, buy=False, price=4.00, vol=50, loc=JITA44, system=JITA),
    order(3, buy=False, price=4.50, vol=1000, loc=JITA44, system=JITA),
    order(4, buy=False, price=3.00, vol=999, loc=JITA_OTHER, system=JITA),          # other station: not ours
    # bids that reach Jita 4-4
    order(10, buy=True, price=4.05, vol=30, loc=JITA44, system=JITA, rng="station"),
    order(11, buy=True, price=4.20, vol=40, loc=JITA_OTHER, system=JITA, rng="solarsystem"),
    order(12, buy=True, price=4.30, vol=500, loc=PERIM_STATION, system=PERIMETER, rng="region", min_volume=100),
    order(13, buy=True, price=4.25, vol=20, loc=PERIM_STATION, system=PERIMETER, rng="1"),
    order(14, buy=True, price=4.01, vol=10, loc=PERIM_STATION, system=PERIMETER, rng="constellation"),
    # bids that don't reach Jita 4-4
    order(20, buy=True, price=9.00, vol=10, loc=JITA_OTHER, system=JITA, rng="station"),
    order(21, buy=True, price=9.00, vol=10, loc=FAR_STATION, system=NEXT_DOOR, rng="1"),
    order(22, buy=True, price=9.00, vol=10, loc=FAR_STATION, system=NEXT_DOOR, rng="constellation"),
    order(23, buy=True, price=9.00, vol=10, loc=30000999, system=30000999, region=OTHER, rng="region"),
    order(24, buy=False, price=1.00, vol=10, loc=60099999, system=30000999, region=OTHER),
]


class WalkTests(unittest.TestCase):
    def test_asks_low_to_high_and_partial_fill(self):
        asks = [Order(4.0, 50, 1, 2, JITA44), Order(4.1, 100, 1, 1, JITA44)]
        f = walk(asks, 120, respect_min_volume=False)
        self.assertEqual((f.filled, f.orders_used, f.best, f.worst, f.depth_total), (120, 2, 4.0, 4.1, 150))
        self.assertAlmostEqual(f.vwap, (50 * 4.0 + 70 * 4.1) / 120)
        f = walk(asks, 1000, respect_min_volume=False)
        self.assertEqual((f.requested, f.filled), (1000, 150))  # partial: the client values the rest

    def test_min_volume_skips_orders_bigger_than_whats_left(self):
        bids = [Order(4.3, 500, 100, 12, 0), Order(4.2, 40, 1, 11, 0)]
        f = walk(bids, 60, respect_min_volume=True)
        self.assertEqual((f.filled, f.best), (40, 4.2))  # 60 < 100: the 4.30 order can't be used
        f = walk(bids, 150, respect_min_volume=True)
        self.assertEqual((f.filled, f.best, f.worst), (150, 4.3, 4.3))
        f = walk([Order(5.0, 3, 10, 1, 0)], 3, respect_min_volume=True)
        self.assertEqual(f.filled, 3)  # min volume above what's left on the order: the remainder can go

    def test_parse_lines(self):
        self.assertEqual(parse_lines([{"type_id": 34, "qty": 5, "side": "sell"}])[0]["levels"], False)
        for bad in ([], [{"type_id": 34, "qty": 0, "side": "sell"}], [{"type_id": 34, "qty": 5, "side": "x"}],
                    [{"type_id": True, "qty": 5, "side": "buy"}], {"type_id": 34}):
            with self.assertRaises(QuoteError):
                parse_lines(bad)


class BookTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        cls.sde = fake_sde(root / "sde")
        cls.snap = root / "orders-1000.jsonl"
        write_jsonl(cls.snap, SNAPSHOT)
        cls.resolver = LocationResolver(cls.sde)
        cls.jumps = JumpGraph(cls.sde)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def book(self, lid):
        idx = BookIndex(self.resolver, self.jumps)
        return build_book(self.snap, 1000, self.resolver.classify(lid), idx.coverage, self.jumps)

    def test_station_scope_uses_reachable_buy_orders(self):
        b = self.book(JITA44)
        self.assertEqual([o.order_id for o in b.orders(TRIT, False)], [2, 1, 3])
        self.assertEqual([o.order_id for o in b.orders(TRIT, True)], [12, 13, 11, 10, 14])

    def test_quote_payload(self):
        b = self.book(JITA44)
        out = quote(b, parse_lines([{"type_id": TRIT, "qty": 120, "side": "buy", "levels": True},
                                    {"type_id": TRIT, "qty": 90, "side": "sell"}]))
        buy = out["34:buy"]
        self.assertEqual((buy["filled"], buy["best"], buy["worst"], buy["orders_used"]), ("120", "4", "4.1", "2"))
        self.assertEqual(buy["levels"], [["4", "50", "1"], ["4.1", "100", "1"]])
        sell = out["34:sell"]  # 90 < 100 skips the 4.30 bid: 20 @ 4.25 + 40 @ 4.20 + 30 @ 4.05
        self.assertEqual((sell["filled"], sell["best"], sell["worst"]), ("90", "4.25", "4.05"))
        self.assertAlmostEqual(float(sell["vwap"]), (20 * 4.25 + 40 * 4.20 + 30 * 4.05) / 90, places=6)
        self.assertEqual(sell["snapshot_unix"], 1000)

    def test_region_scope_and_needle_is_terminated(self):
        b = self.book(FORGE)
        self.assertNotIn(23, [o.order_id for o in b.orders(TRIT, True)])  # region 10000020 != 10000002
        self.assertEqual(len(b.orders(TRIT, False)), 4)
        self.assertEqual([o.order_id for o in self.book(OTHER).orders(TRIT, False)], [24])

    def test_index_caches_per_snapshot(self):
        idx = BookIndex(self.resolver, self.jumps, max_books=1)
        info = self.resolver.classify(JITA44)
        a = idx.get(self.snap, 1000, info)
        self.assertIs(idx.get(self.snap, 1000, info), a)
        idx.get(self.snap, 1000, self.resolver.classify(FORGE))
        self.assertIsNot(idx.get(self.snap, 1000, info), a)  # evicted (max_books=1)
        self.assertEqual(idx.builds, 3)


class FakeResp:
    def __init__(self, status, payload):
        self.status_code, self._payload, self.ok, self.headers = status, payload, status == 200, {}

    def json(self):
        return self._payload


class FakeEsi:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None, headers=None):
        self.calls.append((url, dict(params or {})))
        if "/markets/prices/" in url:
            return FakeResp(200, [{"type_id": 34, "adjusted_price": 4.0, "average_price": 4.1}])
        tid = (params or {}).get("type_id")
        if tid == 999:
            return FakeResp(404, {"error": "not found"})
        from datetime import datetime, timedelta, timezone

        today = datetime.now(timezone.utc).date()
        rows = []
        for back in range(40, 0, -1):  # ESI's newest day is yesterday
            d = (today - timedelta(days=back)).isoformat()
            rows.append({"date": d, "average": 4.0 + back / 100, "highest": 5, "lowest": 3,
                         "volume": 1000, "order_count": 10})
        return FakeResp(200, rows)


class HistoryTests(unittest.TestCase):
    def test_cache_until_rollover_and_summaries(self):
        with tempfile.TemporaryDirectory() as d:
            esi = FakeEsi()
            store = EsiHistoryStore(Path(d), client=esi)
            out = store.get(FORGE, [34, 999], days=30)
            self.assertEqual(len(esi.calls), 2)
            self.assertEqual(len(out["34"]["days"]), 30)
            self.assertEqual(out["34"]["adv_30d"], "1000")
            self.assertEqual(out["999"]["days"], [])
            store.get(FORGE, [34, 999])
            self.assertEqual(len(esi.calls), 2)  # both cached (404 backs off)
            store._db.execute("UPDATE fetched SET fetched_at=?", (last_rollover() - 1,))
            store.get(FORGE, [34])
            self.assertEqual(len(esi.calls), 3)  # older than the last rollover: refetched
            self.assertEqual(store.stats()["pairs"], 2)
            store.close()

    def test_summarize_counts_missing_days_as_zero_volume(self):
        rows = [("2026-11-01", 10.0, 11, 9, 100, 1), ("2026-11-03", 20.0, 21, 19, 300, 1)]
        s = summarize(rows, 30, None)
        self.assertEqual(s["adv_30d"], "13.333333333")
        self.assertEqual(s["vwavg_30d"], "17.5")
        self.assertEqual(s["median_30d"], "15")
        self.assertEqual(s["last_date"], "2026-11-03")


class HttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from eveMarket.server import serve
        from eveMarket.snapshot import contracts_path, orders_path

        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        sde = fake_sde(root / "sde")
        data = root / "data"
        data.mkdir()
        write_jsonl(orders_path(data, 1000), SNAPSHOT)
        write_jsonl(contracts_path(data, 2000), [
            {"contract_id": 1, "type": "item_exchange", "region_id": FORGE, "start_location_id": JITA44,
             "price": 5e6, "date_issued": "2026-11-01T00:00:00Z", "date_expired": "2099-01-01T00:00:00Z"},
            {"contract_id": 2, "type": "item_exchange", "region_id": FORGE, "start_location_id": JITA44,
             "price": 9e6, "date_issued": "2026-11-01T00:00:00Z", "date_expired": "2099-01-01T00:00:00Z"},
            {"contract_id": 3, "type": "courier", "region_id": FORGE, "start_location_id": JITA44,
             "end_location_id": JITA_OTHER, "date_expired": "2099-01-01T00:00:00Z"},
        ])
        items_dir = data / "contracts" / "items"
        items_dir.mkdir(parents=True, exist_ok=True)
        write_jsonl(items_dir / "items.jsonl", [
            {"contract_id": 1, "items": [{"type_id": 691, "quantity": 1, "is_included": True,
                                          "is_blueprint_copy": True, "material_efficiency": 10,
                                          "time_efficiency": 20, "runs": 10, "record_id": 1}]},
            {"contract_id": 2, "items": [{"type_id": 34, "quantity": 1000, "is_included": True, "record_id": 2}]},
        ])
        cls.esi = FakeEsi()
        cls.history = EsiHistoryStore(data, client=cls.esi)
        cls.httpd = serve(sde_dir=sde, data_dir=data, bind="127.0.0.1", port=0, client=cls.esi, history=cls.history)
        cls.port = cls.httpd.server_address[1]
        cls.thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.history.close()
        cls.tmp.cleanup()

    def call(self, method, path, body=None):
        req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", method=method,
                                     data=json.dumps(body).encode() if body is not None else None,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read()
                if r.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                return r.status, dict(r.headers), raw
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers), e.read()

    def test_quote(self):
        code, hdrs, raw = self.call("POST", f"/quote/{JITA44}", [{"type_id": TRIT, "qty": 120, "side": "buy"}])
        self.assertEqual(code, 200)
        self.assertEqual(hdrs["X-Snapshot-Unix"], "1000")
        self.assertEqual(json.loads(raw)["34:buy"]["filled"], "120")
        self.assertEqual(self.call("POST", f"/quote/{JITA44}", [{"type_id": TRIT}])[0], 400)
        self.assertEqual(self.call("POST", "/quote/5", [{"type_id": TRIT, "qty": 1, "side": "buy"}])[0], 404)

    def test_item_exchange_and_bpc(self):
        code, hdrs, raw = self.call("GET", "/contracts/itemExchange")
        rows = [json.loads(x) for x in raw.decode().splitlines()]
        self.assertEqual(code, 200)
        self.assertEqual([r["contract_id"] for r in rows], [1, 2])
        self.assertEqual(rows[0]["location_id"], JITA44)
        self.assertEqual(rows[0]["items"][0]["runs"], 10)
        self.assertIs(rows[1]["items"][0]["is_blueprint_copy"], False)
        _, _, raw = self.call("GET", "/contracts/itemExchange?type_id=34")
        self.assertEqual([json.loads(x)["contract_id"] for x in raw.decode().splitlines()], [2])
        code, _, raw = self.call("GET", "/contracts/bpc?type_id=691")
        bpc = json.loads(raw)
        self.assertEqual((code, bpc["contracts"], bpc["per_run"]["median"]), (200, 1, 5e5))

    def test_esi_history_and_health(self):
        code, _, raw = self.call("POST", f"/esiHistory/{FORGE}?days=7", [34])
        self.assertEqual(code, 200)
        self.assertEqual(len(json.loads(raw)["34"]["days"]), 7)
        self.assertEqual(self.call("POST", "/esiHistory/30000142", [34])[0], 400)
        health = json.loads(self.call("GET", "/health")[2])
        self.assertTrue(health["ok"])
        self.assertGreaterEqual(health["history"]["pairs"], 1)

    def test_refdata_proxy(self):
        code, hdrs, raw = self.call("GET", "/esi/adjustedPrices")
        self.assertEqual((code, json.loads(raw)[0]["type_id"]), (200, 34))
        self.assertIn("X-Fetched-Unix", hdrs)
        self.assertEqual(self.call("GET", "/esi/nope")[0], 404)


if __name__ == "__main__":
    unittest.main()
