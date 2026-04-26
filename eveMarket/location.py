"""SDE-backed location classification.

Resolves any EVE location ID (region / constellation / system / NPC station /
player structure) to the set of region / system / station IDs it covers.

EVE ID ranges (used as fast prefilter, then verified against SDE):
  region:        10_000_000 - 10_999_999
  constellation: 20_000_000 - 20_999_999
  system:        30_000_000 - 30_999_999
  NPC station:   60_000_000 - 63_999_999
  structure:    >= 1_000_000_000_000 (1e12)
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class LocationInfo:
    kind: str                       # region|constellation|system|station|structure|unknown
    location_id: int
    region_ids: set[int] = field(default_factory=set)
    system_ids: set[int] = field(default_factory=set)
    station_ids: set[int] = field(default_factory=set)
    structure_ids: set[int] = field(default_factory=set)


class LocationResolver:
    """Loads SDE maps once and answers classify(location_id) queries."""

    def __init__(self, sde_dir: Path) -> None:
        self.sde_dir = Path(sde_dir)
        self.systems_in_region: dict[int, set[int]] = {}
        self.systems_in_constellation: dict[int, set[int]] = {}
        self.region_of_system: dict[int, int] = {}
        self.system_of_station: dict[int, int] = {}
        self.region_of_constellation: dict[int, int] = {}
        self._load()

    def _load(self) -> None:
        sys_path = self.sde_dir / "mapSolarSystems.jsonl"
        with sys_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                sid = row.get("_key") or row.get("solarSystemID") or row.get("systemID")
                rid = row.get("regionID")
                if sid is None or rid is None:
                    continue
                sid = int(sid); rid = int(rid)
                self.region_of_system[sid] = rid
                self.systems_in_region.setdefault(rid, set()).add(sid)
                cid = row.get("constellationID")
                if cid is not None:
                    cid = int(cid)
                    self.systems_in_constellation.setdefault(cid, set()).add(sid)
                    self.region_of_constellation.setdefault(cid, rid)

        cons_path = self.sde_dir / "mapConstellations.jsonl"
        with cons_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                cid = row.get("_key") or row.get("constellationID")
                rid = row.get("regionID")
                if cid is None:
                    continue
                cid = int(cid)
                if rid is not None:
                    self.region_of_constellation[cid] = int(rid)
                ssids = row.get("solarSystemIDs") or []
                for s in ssids:
                    self.systems_in_constellation.setdefault(cid, set()).add(int(s))

        sta_path = self.sde_dir / "npcStations.jsonl"
        with sta_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                stid = row.get("_key") or row.get("stationID")
                sid = row.get("solarSystemID")
                if stid is None or sid is None:
                    continue
                self.system_of_station[int(stid)] = int(sid)

        logger.info("LocationResolver loaded: %s systems, %s constellations, %s NPC stations",
                    len(self.region_of_system),
                    len(self.systems_in_constellation),
                    len(self.system_of_station))

    def classify(self, location_id: int) -> LocationInfo:
        lid = int(location_id)
        info = LocationInfo(kind="unknown", location_id=lid)

        if 10_000_000 <= lid <= 10_999_999:
            info.kind = "region"
            info.region_ids = {lid}
            info.system_ids = set(self.systems_in_region.get(lid, set()))
            return info

        if 20_000_000 <= lid <= 20_999_999:
            info.kind = "constellation"
            rid = self.region_of_constellation.get(lid)
            if rid is not None:
                info.region_ids = {rid}
            info.system_ids = set(self.systems_in_constellation.get(lid, set()))
            return info

        if 30_000_000 <= lid <= 30_999_999:
            info.kind = "system"
            info.system_ids = {lid}
            rid = self.region_of_system.get(lid)
            if rid is not None:
                info.region_ids = {rid}
            return info

        if 60_000_000 <= lid <= 63_999_999:
            info.kind = "station"
            info.station_ids = {lid}
            sid = self.system_of_station.get(lid)
            if sid is not None:
                info.system_ids = {sid}
                rid = self.region_of_system.get(sid)
                if rid is not None:
                    info.region_ids = {rid}
            return info

        if lid >= 1_000_000_000_000:
            info.kind = "structure"
            info.structure_ids = {lid}
            return info

        return info

    def region_for_system(self, system_id: int) -> Optional[int]:
        return self.region_of_system.get(int(system_id))
