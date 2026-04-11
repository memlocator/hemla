"""
On-demand transit travel time engine using GTFS Connection Scan Algorithm (reverse CSA).

Given a destination (lat, lon) and departure time, returns travel times from every
DeSO zone centroid to that destination via SL transit.

Algorithm: Reverse CSA
- Sort connections by departure time descending
- Scan backwards: find latest departure from each stop to arrive at destination by target_arrival
- One pass = all-stops solution
- Walk from DeSO centroid to nearest stop (≤800m), walk from final stop to destination

Usage (module):
    engine = TransitEngine.load(gtfs_zip, deso_csv, date="20260310", arrival_hhmm="08:45")
    times = engine.travel_times_to(dest_lat, dest_lon)
    # → {deso_id: minutes, ...}
"""
from __future__ import annotations

import csv
import io
import math
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Optional


WALK_SPEED_MS = 5000 / 3600   # 5 km/h in m/s
MAX_WALK_M = 800
TRANSFER_SEC = 120             # default transfer penalty


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6_371_000.0
    a1, a2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    h = math.sin(dlat / 2) ** 2 + math.cos(a1) * math.cos(a2) * math.sin(dlon / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(h), math.sqrt(max(0.0, 1 - h)))


def _time_to_sec(t: str) -> Optional[int]:
    try:
        p = t.strip().split(":")
        return int(p[0]) * 3600 + int(p[1]) * 60 + int(p[2])
    except Exception:
        return None


class TransitEngine:
    """
    Loaded once at app startup. Holds GTFS connections and stop data in memory.
    ~120MB RAM, loads in ~5s.
    """

    def __init__(
        self,
        stops: dict,                    # stop_id → {lat, lon, name, parent}
        connections_fwd: list,          # sorted asc by dep_sec: (dep, arr, from_s, to_s, trip_id)
        connections_rev: list,          # sorted desc by dep_sec (same tuples)
        footpaths: dict,                # stop_id → [(to_stop, sec)]
        footpaths_rev: dict,            # reverse footpaths: to_stop → [(from_stop, sec)]
        deso_zones: list,               # [{id, lat, lon, nearest_stop_id, nearest_stop_dist_m}]
        date: str,
        arrival_hhmm: str,
    ):
        self.stops = stops
        self.connections_fwd = connections_fwd
        self.connections_rev = connections_rev
        self.footpaths = footpaths
        self.footpaths_rev = footpaths_rev
        self.deso_zones = deso_zones
        self.date = date
        self.arrival_sec = self._parse_arrival(arrival_hhmm)
        self._cache: dict[tuple, dict[str, int]] = {}

    @staticmethod
    def _parse_arrival(hhmm: str) -> int:
        h, m = map(int, hhmm.split(":"))
        return h * 3600 + m * 60

    @classmethod
    def load(
        cls,
        gtfs_zip: Path,
        deso_csv: Path,
        date: str | None = None,
        arrival_hhmm: str = "08:45",
    ) -> "TransitEngine":
        import datetime

        if date is None:
            today = datetime.date.today()
            days_ahead = (1 - today.weekday()) % 7 or 7
            date = (today + datetime.timedelta(days=days_ahead)).strftime("%Y%m%d")

        arr_h, arr_m = map(int, arrival_hhmm.split(":"))
        arrival_sec = arr_h * 3600 + arr_m * 60
        # Load connections in window [arrival - 3h, arrival + 30min]
        window_start = arrival_sec - 3 * 3600
        window_end = arrival_sec + 30 * 60

        print(f"[transit] Loading GTFS for date={date} arrival={arrival_hhmm}...", flush=True)

        with zipfile.ZipFile(gtfs_zip) as z:
            raw_cal = z.read("calendar_dates.txt").decode("utf-8-sig")
            raw_trips = z.read("trips.txt").decode("utf-8-sig")
            raw_st = z.read("stop_times.txt").decode("utf-8-sig")
            raw_stops = z.read("stops.txt").decode("utf-8-sig")
            raw_transfers = z.read("transfers.txt").decode("utf-8-sig")

        # Active services
        active_services = {
            r["service_id"] for r in csv.DictReader(io.StringIO(raw_cal))
            if r["date"] == date and r["exception_type"] == "1"
        }
        active_trips = {
            r["trip_id"] for r in csv.DictReader(io.StringIO(raw_trips))
            if r["service_id"] in active_services
        }
        print(f"[transit]   active trips={len(active_trips)}", flush=True)

        # Stops
        stops: dict[str, dict] = {}
        parent_children: dict[str, list[str]] = defaultdict(list)
        for row in csv.DictReader(io.StringIO(raw_stops)):
            try:
                lat, lon = float(row["stop_lat"]), float(row["stop_lon"])
            except ValueError:
                continue
            stops[row["stop_id"]] = {
                "lat": lat, "lon": lon,
                "name": row.get("stop_name", ""),
                "parent": row.get("parent_station", ""),
            }
            if row.get("parent_station"):
                parent_children[row["parent_station"]].append(row["stop_id"])

        # Parse stop_times grouped by trip
        trip_stops: dict[str, list] = defaultdict(list)
        for row in csv.DictReader(io.StringIO(raw_st)):
            tid = row["trip_id"]
            if tid not in active_trips:
                continue
            dep = _time_to_sec(row["departure_time"])
            arr = _time_to_sec(row["arrival_time"])
            if dep is None or arr is None:
                continue
            if dep > window_end or arr < window_start:
                continue
            try:
                seq = int(row["stop_sequence"])
            except ValueError:
                continue
            trip_stops[tid].append((seq, dep, arr, row["stop_id"]))

        # Build connections
        connections: list[tuple] = []
        for tid, events in trip_stops.items():
            events.sort()
            for i in range(len(events) - 1):
                _, dep, _, from_s = events[i]
                _, _, arr, to_s = events[i + 1]
                if from_s in stops and to_s in stops:
                    connections.append((dep, arr, from_s, to_s, tid))

        connections_fwd = sorted(connections)
        connections_rev = sorted(connections, reverse=True)
        print(f"[transit]   connections={len(connections_fwd)}", flush=True)

        # Footpaths
        footpaths: dict[str, list] = defaultdict(list)
        footpaths_rev: dict[str, list] = defaultdict(list)

        for row in csv.DictReader(io.StringIO(raw_transfers)):
            fs, ts = row["from_stop_id"], row["to_stop_id"]
            if fs not in stops or ts not in stops or fs == ts:
                continue
            if row.get("from_trip_id") or row.get("to_trip_id"):
                continue
            try:
                t = int(row["min_transfer_time"]) if row.get("min_transfer_time") else TRANSFER_SEC
            except ValueError:
                t = TRANSFER_SEC
            footpaths[fs].append((ts, t))
            footpaths_rev[ts].append((fs, t))

        # Same-parent-station transfers
        for parent, children in parent_children.items():
            for i, a in enumerate(children):
                for b in children[i + 1:]:
                    if a in stops and b in stops:
                        footpaths[a].append((b, TRANSFER_SEC))
                        footpaths[b].append((a, TRANSFER_SEC))
                        footpaths_rev[b].append((a, TRANSFER_SEC))
                        footpaths_rev[a].append((b, TRANSFER_SEC))

        print(f"[transit]   footpaths={sum(len(v) for v in footpaths.values())}", flush=True)

        # DeSO zones with nearest stop
        active_stop_ids = set()
        for _, _, fs, ts, _ in connections_fwd:
            active_stop_ids.add(fs)
            active_stop_ids.add(ts)
        active_stops = {sid: s for sid, s in stops.items() if sid in active_stop_ids}

        deso_zones = []
        for row in csv.DictReader(deso_csv.open("r", encoding="utf-8")):
            lat, lon = float(row["lat"]), float(row["lon"])
            # Find nearest active stop within MAX_WALK_M
            best_id, best_d = None, float("inf")
            for sid, s in active_stops.items():
                d = _haversine_m(lat, lon, s["lat"], s["lon"])
                if d < best_d:
                    best_d, best_id = d, sid
            if best_d <= MAX_WALK_M:
                deso_zones.append({
                    "id": row["id"],
                    "lat": lat,
                    "lon": lon,
                    "stop_id": best_id,
                    "walk_sec": int(best_d / WALK_SPEED_MS),
                })
            else:
                deso_zones.append({"id": row["id"], "lat": lat, "lon": lon, "stop_id": None, "walk_sec": 0})

        reachable = sum(1 for z in deso_zones if z["stop_id"])
        print(f"[transit]   deso zones={len(deso_zones)} with nearby stop={reachable}", flush=True)
        print("[transit] Ready.", flush=True)

        return cls(stops, connections_fwd, connections_rev, footpaths, footpaths_rev,
                   deso_zones, date, arrival_hhmm)

    def _nearest_stops(self, lat: float, lon: float, max_m: float = MAX_WALK_M) -> list[tuple[str, int]]:
        """Returns list of (stop_id, walk_sec) for all active stops within max_m."""
        result = []
        active = set()
        for _, _, fs, ts, _ in self.connections_fwd[:100]:  # quick sample to get active set
            active.add(fs); active.add(ts)
        # Actually build full active set once
        if not hasattr(self, "_active_stops"):
            s = set()
            for _, _, fs, ts, _ in self.connections_fwd:
                s.add(fs); s.add(ts)
            self._active_stops = s

        for sid in self._active_stops:
            s = self.stops.get(sid)
            if s is None:
                continue
            d = _haversine_m(lat, lon, s["lat"], s["lon"])
            if d <= max_m:
                result.append((sid, int(d / WALK_SPEED_MS)))
        return result

    def _reverse_csa(self, dest_stops: list[tuple[str, int]]) -> dict[str, int]:
        """
        Reverse CSA: find latest departure from every stop to arrive at dest_stops
        by self.arrival_sec.

        Returns {stop_id: latest_departure_sec_to_arrive_in_time}
        We convert this to travel_minutes = arrival_sec - latest_departure + walk_to_dest
        """
        INF = 10 ** 9
        # T[stop] = latest time you can depart from stop and still reach dest
        T: dict[str, int] = {}

        # Initialise destination stops
        for sid, walk_sec in dest_stops:
            latest_dep = self.arrival_sec - walk_sec
            if T.get(sid, -INF) < latest_dep:
                T[sid] = latest_dep
                # Reverse footpaths: if you can be at sid by latest_dep,
                # you can also be at footpath sources by latest_dep - ft
                for (nb, ft) in self.footpaths_rev.get(sid, []):
                    nb_dep = latest_dep - ft
                    if T.get(nb, -INF) < nb_dep:
                        T[nb] = nb_dep

        # Reverse trip tracker: T_trip[trip_id] = latest time you can arrive on this trip
        T_trip: dict[str, int] = {}

        for dep_sec, arr_sec, from_s, to_s, trip_id in self.connections_rev:
            if arr_sec > self.arrival_sec:
                continue

            # Can we use this connection to reach the destination?
            boarded = trip_id in T_trip
            if not boarded:
                if T.get(to_s, -INF) >= arr_sec:
                    T_trip[trip_id] = arr_sec
                    boarded = True

            if boarded:
                if T.get(from_s, -INF) < dep_sec:
                    T[from_s] = dep_sec
                    for (nb, ft) in self.footpaths_rev.get(from_s, []):
                        nb_dep = dep_sec - ft
                        if T.get(nb, -INF) < nb_dep:
                            T[nb] = nb_dep

        return T

    def travel_times_to(self, dest_lat: float, dest_lon: float) -> dict[str, int]:
        """
        Returns {deso_id: minutes} for all zones that can reach (dest_lat, dest_lon)
        by self.arrival_sec.
        """
        # Round destination to ~100m grid for caching
        cache_key = (round(dest_lat, 3), round(dest_lon, 3))
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Find stops near destination
        dest_stops = self._nearest_stops(dest_lat, dest_lon)
        if not dest_stops:
            # No transit near destination — return empty (caller should fall back)
            self._cache[cache_key] = {}
            return {}

        # Run reverse CSA
        T = self._reverse_csa(dest_stops)

        # Map to DeSO zones
        result: dict[str, int] = {}
        for zone in self.deso_zones:
            sid = zone["stop_id"]
            if sid is None:
                continue
            latest_dep = T.get(sid)
            if latest_dep is None:
                continue
            travel_sec = self.arrival_sec - latest_dep + zone["walk_sec"]
            if travel_sec < 60 or travel_sec > 3 * 3600:
                continue
            result[zone["id"]] = max(1, round(travel_sec / 60))

        self._cache[cache_key] = result
        return result
