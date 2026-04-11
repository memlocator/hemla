"""
Build a transit travel-time matrix between DeSO zone clusters using GTFS CSA.

Steps:
1. Parse GTFS stop_times for Tuesday rush hour (07:00-10:00) → sorted connections
2. Build stop-level walking graph (transfers + same parent station)
3. Cluster DeSO zones at 1 km radius → ~300 nodes
4. For each cluster, find nearest GTFS stop within 800 m
5. Run CSA (Connection Scan Algorithm) from each cluster's stop at 08:15
6. Output: data/raw/deso_travel_matrix.csv  (from_cluster, to_cluster, minutes)
         data/raw/deso_clusters.csv         (cluster_id, lat, lon, members...)

Usage:
    python build_transit_graph.py [--date YYYYMMDD] [--depart HH:MM] [--max-walk-m 800]
"""
from __future__ import annotations

import argparse
import csv
import io
import math
import zipfile
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
GTFS_ZIP = RAW / "sl_gtfs.zip"
DESO_CSV = ROOT / "data" / "deso_stockholm_areas.csv"
OUT_MATRIX = RAW / "deso_travel_matrix.csv"
OUT_CLUSTERS = RAW / "deso_clusters.csv"

CLUSTER_RADIUS_KM = 1.0
MAX_WALK_M = 800
WALK_SPEED_MS = 5000 / 3600  # 5 km/h in m/s
MAX_WALK_SEC = int(MAX_WALK_M / WALK_SPEED_MS)  # ~576 s
TRANSFER_WALK_SEC = 120  # default transfer penalty if not in transfers.txt


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6_371_000.0
    a1, a2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    h = math.sin(dlat / 2) ** 2 + math.cos(a1) * math.cos(a2) * math.sin(dlon / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(h), math.sqrt(max(0.0, 1 - h)))


def time_to_sec(t: str) -> int | None:
    """HH:MM:SS → seconds. Handles >24h times (GTFS allows 25:xx for overnight)."""
    try:
        parts = t.strip().split(":")
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except Exception:
        return None


def sec_to_min(s: int) -> int:
    return int(round(s / 60))


# ---------------------------------------------------------------------------
# GTFS loading
# ---------------------------------------------------------------------------

def load_gtfs(zip_path: Path, target_date: str, depart_hhmm: str):
    """
    Returns:
      stops: {stop_id: {lat, lon, name, parent}}
      connections: sorted list of (dep_sec, arr_sec, from_stop, to_stop, trip_id)
      footpaths: {stop_id: [(to_stop_id, walk_sec), ...]}  (transfers + same-parent)
    """
    print("Loading GTFS...", flush=True)
    with zipfile.ZipFile(zip_path) as z:
        raw_stops      = z.read("stops.txt").decode("utf-8-sig")
        raw_routes     = z.read("routes.txt").decode("utf-8-sig")
        raw_trips      = z.read("trips.txt").decode("utf-8-sig")
        raw_st         = z.read("stop_times.txt").decode("utf-8-sig")
        raw_cal        = z.read("calendar_dates.txt").decode("utf-8-sig")
        raw_transfers  = z.read("transfers.txt").decode("utf-8-sig")

    # Active services on target date
    active_services: set[str] = set()
    for row in csv.DictReader(io.StringIO(raw_cal)):
        if row["date"] == target_date and row["exception_type"] == "1":
            active_services.add(row["service_id"])
    print(f"  active services on {target_date}: {len(active_services)}", flush=True)

    # Active trip ids
    active_trips: set[str] = set()
    trip_route: dict[str, str] = {}
    for row in csv.DictReader(io.StringIO(raw_trips)):
        if row["service_id"] in active_services:
            active_trips.add(row["trip_id"])
            trip_route[row["trip_id"]] = row["route_id"]
    print(f"  active trips: {len(active_trips)}", flush=True)

    # Stops
    stops: dict[str, dict] = {}
    parent_children: dict[str, list[str]] = defaultdict(list)
    for row in csv.DictReader(io.StringIO(raw_stops)):
        try:
            lat = float(row["stop_lat"])
            lon = float(row["stop_lon"])
        except ValueError:
            continue
        stops[row["stop_id"]] = {
            "lat": lat, "lon": lon,
            "name": row.get("stop_name", ""),
            "parent": row.get("parent_station", ""),
        }
        if row.get("parent_station"):
            parent_children[row["parent_station"]].append(row["stop_id"])

    # Rush-hour window: depart_hhmm - 30min to depart + 3h
    dep_h, dep_m = map(int, depart_hhmm.split(":"))
    window_start = (dep_h - 0) * 3600 + dep_m * 60  # from query time
    window_end = window_start + 3 * 3600              # up to 3h later

    # Parse stop_times → connections (consecutive stops in same trip)
    print("  parsing stop_times...", flush=True)
    # Group by trip_id
    trip_stops: dict[str, list[tuple[int, int, int, str]]] = defaultdict(list)
    # (seq, dep_sec, arr_sec, stop_id)
    for row in csv.DictReader(io.StringIO(raw_st)):
        tid = row["trip_id"]
        if tid not in active_trips:
            continue
        dep = time_to_sec(row["departure_time"])
        arr = time_to_sec(row["arrival_time"])
        if dep is None or arr is None:
            continue
        if dep > window_end:
            continue
        try:
            seq = int(row["stop_sequence"])
        except ValueError:
            continue
        trip_stops[tid].append((seq, dep, arr, row["stop_id"]))

    print(f"  trips with stop_times: {len(trip_stops)}", flush=True)

    # Build connections from consecutive trip stops
    connections: list[tuple[int, int, str, str, str]] = []
    # (dep_sec, arr_sec, from_stop, to_stop, trip_id)
    for tid, events in trip_stops.items():
        events.sort()
        for i in range(len(events) - 1):
            _, dep, _, from_s = events[i]
            _, _, arr, to_s = events[i + 1]
            if dep >= window_start and arr <= window_end and from_s in stops and to_s in stops:
                connections.append((dep, arr, from_s, to_s, tid))

    connections.sort()
    print(f"  connections in window: {len(connections)}", flush=True)

    # Footpaths: from transfers.txt (type 2 = timed, type 0/1 = guaranteed)
    footpaths: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for row in csv.DictReader(io.StringIO(raw_transfers)):
        from_s = row["from_stop_id"]
        to_s = row["to_stop_id"]
        if from_s not in stops or to_s not in stops:
            continue
        if from_s == to_s:
            continue
        # Only use non-trip-specific transfers
        if row.get("from_trip_id") or row.get("to_trip_id"):
            continue
        try:
            t = int(row["min_transfer_time"]) if row.get("min_transfer_time") else TRANSFER_WALK_SEC
        except ValueError:
            t = TRANSFER_WALK_SEC
        footpaths[from_s].append((to_s, t))

    # Also add same-parent-station footpaths
    for parent, children in parent_children.items():
        for i, a in enumerate(children):
            for b in children[i + 1:]:
                if a in stops and b in stops:
                    footpaths[a].append((b, TRANSFER_WALK_SEC))
                    footpaths[b].append((a, TRANSFER_WALK_SEC))

    print(f"  footpath entries: {sum(len(v) for v in footpaths.values())}", flush=True)
    return stops, connections, footpaths


# ---------------------------------------------------------------------------
# CSA (Connection Scan Algorithm)
# ---------------------------------------------------------------------------

def csa(source_stops: list[tuple[str, int]], connections, footpaths, stops, depart_sec: int) -> dict[str, int]:
    """
    Run CSA from source_stops (list of (stop_id, walk_sec_to_reach)) at depart_sec.
    Returns {stop_id: earliest_arrival_sec}.
    """
    INF = 10 ** 9
    T: dict[str, int] = {}  # earliest arrival at each stop

    # Initialise source stops with walking time
    for sid, walk_sec in source_stops:
        arr = depart_sec + walk_sec
        if T.get(sid, INF) > arr:
            T[sid] = arr
            # propagate footpaths from source
            for (nb, ft) in footpaths.get(sid, []):
                nb_arr = arr + ft
                if T.get(nb, INF) > nb_arr:
                    T[nb] = nb_arr

    # Earliest trip time: T_trip[trip_id] = earliest we can board this trip
    T_trip: dict[str, int] = {}

    for dep_sec, arr_sec, from_s, to_s, trip_id in connections:
        if dep_sec < depart_sec:
            continue

        # Can we board this trip at from_s?
        boarded = trip_id in T_trip
        if not boarded:
            if T.get(from_s, INF) <= dep_sec:
                T_trip[trip_id] = dep_sec
                boarded = True

        if boarded:
            if T.get(to_s, INF) > arr_sec:
                T[to_s] = arr_sec
                # Propagate footpaths
                for (nb, ft) in footpaths.get(to_s, []):
                    nb_arr = arr_sec + ft
                    if T.get(nb, INF) > nb_arr:
                        T[nb] = nb_arr

    return T


# ---------------------------------------------------------------------------
# DeSO clustering
# ---------------------------------------------------------------------------

def cluster_deso(rows: list[dict], radius_km: float) -> list[dict]:
    clusters: list[dict] = []
    for row in rows:
        lat = float(row["lat"])
        lon = float(row["lon"])
        mun = row["municipality"].strip().lower()
        matched = None
        for c in clusters:
            if c["municipality"] != mun:
                continue
            d = haversine_m(lat, lon, c["lat"], c["lon"]) / 1000
            if d <= radius_km:
                matched = c
                break
        if matched is None:
            clusters.append({"id": len(clusters), "municipality": mun, "lat": lat, "lon": lon, "members": [row["id"]]})
        else:
            n = len(matched["members"]) + 1
            matched["lat"] = (matched["lat"] * (n - 1) + lat) / n
            matched["lon"] = (matched["lon"] * (n - 1) + lon) / n
            matched["members"].append(row["id"])
    return clusters


def find_nearest_stop(lat: float, lon: float, stops: dict, max_m: float) -> tuple[str | None, float]:
    best_id, best_d = None, float("inf")
    for sid, s in stops.items():
        d = haversine_m(lat, lon, s["lat"], s["lon"])
        if d < best_d:
            best_d = d
            best_id = sid
    if best_d > max_m:
        return None, best_d
    return best_id, best_d


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="YYYYMMDD, default=next Tuesday")
    parser.add_argument("--depart", default="08:15", help="HH:MM departure time")
    parser.add_argument("--max-walk-m", type=float, default=MAX_WALK_M)
    parser.add_argument("--cluster-km", type=float, default=CLUSTER_RADIUS_KM)
    args = parser.parse_args()

    if args.date is None:
        import datetime
        today = datetime.date.today()
        days_ahead = (1 - today.weekday()) % 7 or 7
        args.date = (today + datetime.timedelta(days=days_ahead)).strftime("%Y%m%d")
    print(f"Using date={args.date} depart={args.depart}", flush=True)

    dep_h, dep_m = map(int, args.depart.split(":"))
    depart_sec = dep_h * 3600 + dep_m * 60

    # Load DeSO zones
    deso_rows = list(csv.DictReader(DESO_CSV.open("r", encoding="utf-8")))
    clusters = cluster_deso(deso_rows, radius_km=args.cluster_km)
    print(f"DeSO zones={len(deso_rows)} → clusters={len(clusters)}", flush=True)

    # Load GTFS
    stops, connections, footpaths = load_gtfs(GTFS_ZIP, args.date, args.depart)

    # Find nearest stop for each cluster
    # Use only stops that appear in connections (active stops)
    active_stop_ids = set()
    for _, _, fs, ts, _ in connections:
        active_stop_ids.add(fs)
        active_stop_ids.add(ts)
    active_stops = {sid: s for sid, s in stops.items() if sid in active_stop_ids}
    print(f"Active stops in rush hour window: {len(active_stops)}", flush=True)

    cluster_stops: list[tuple[str | None, float]] = []
    for c in clusters:
        sid, dist = find_nearest_stop(c["lat"], c["lon"], active_stops, args.max_walk_m)
        cluster_stops.append((sid, dist))

    reachable = sum(1 for sid, _ in cluster_stops if sid is not None)
    print(f"Clusters with nearby stop (≤{args.max_walk_m}m): {reachable}/{len(clusters)}", flush=True)

    # Write clusters CSV
    with OUT_CLUSTERS.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cluster_id", "municipality", "lat", "lon", "nearest_stop_id", "nearest_stop_dist_m", "members"])
        for c, (sid, dist) in zip(clusters, cluster_stops):
            w.writerow([c["id"], c["municipality"], f"{c['lat']:.6f}", f"{c['lon']:.6f}",
                        sid or "", f"{dist:.0f}", "|".join(c["members"])])

    # For each cluster that has a stop, run CSA and record travel times to all other clusters
    print(f"\nRunning CSA for {len(clusters)} source clusters...", flush=True)

    # Build cluster stop lookup: cluster_id → (stop_id, walk_sec)
    cluster_source: list[tuple[str | None, int]] = []
    for sid, dist in cluster_stops:
        if sid is None:
            cluster_source.append((None, 0))
        else:
            walk_sec = int(dist / WALK_SPEED_MS)
            cluster_source.append((sid, walk_sec))

    # We'll output the matrix row by row
    with OUT_MATRIX.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["from_cluster", "to_cluster", "minutes"])

        for ci, c in enumerate(clusters):
            src_stop, src_walk = cluster_source[ci]
            if src_stop is None:
                if (ci + 1) % 50 == 0:
                    print(f"  {ci+1}/{len(clusters)} (no stop)", flush=True)
                continue

            arrivals = csa(
                [(src_stop, src_walk)],
                connections, footpaths, stops, depart_sec
            )

            # For each destination cluster
            for cj, dc in enumerate(clusters):
                dst_stop, dst_walk = cluster_source[cj]
                if dst_stop is None:
                    continue
                # Find best arrival across the destination cluster's stop + footpaths
                arr = arrivals.get(dst_stop)
                if arr is None:
                    continue
                total_sec = arr - depart_sec + dst_walk
                if total_sec < 0:
                    continue
                minutes = max(1, sec_to_min(total_sec))
                w.writerow([c["id"], dc["id"], minutes])

            if (ci + 1) % 10 == 0 or ci + 1 == len(clusters):
                print(f"  {ci+1}/{len(clusters)}", flush=True)

    # Summary
    total_pairs = sum(1 for _ in open(OUT_MATRIX) if _ and _[0].isdigit())
    print(f"\nwrote {OUT_MATRIX}  pairs={total_pairs}")
    print(f"wrote {OUT_CLUSTERS}  clusters={len(clusters)}")


if __name__ == "__main__":
    main()
