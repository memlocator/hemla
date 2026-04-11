"""
Derive transit type and nearest station for each DeSO zone from GTFS static data.

For each DeSO centroid, finds the nearest stop reachable by subway (401),
commuter rail (100), or tram (900) within 800m. Classifies each zone:
  - "subway"      — tunnelbana within 800m
  - "commuter_rail" — pendeltåg within 800m (and no subway)
  - "tram"        — spårväg within 800m (and no above)
  - "bus_only"    — only bus/ferry within 800m
  - "none"        — no SL stop within 800m

Also records nearest_station_name and nearest_station_walk_min (straight-line
at 5 km/h walking speed, capped to 15 min).

Input:  backend/data/raw/sl_gtfs.zip
        backend/data/deso_stockholm_areas.csv
Output: backend/data/raw/deso_transit_type.csv
        Columns: desokod, transit_type, nearest_station_name,
                 nearest_station_walk_min, nearest_station_dist_m
"""
from __future__ import annotations

import csv
import io
import math
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"

GTFS_ZIP = RAW / "sl_gtfs.zip"
IN_CSV = ROOT / "data" / "deso_stockholm_areas.csv"
OUT_CSV = RAW / "deso_transit_type.csv"

WALK_SPEED_KMH = 5.0
MAX_WALK_M = 800  # only consider stops within 800m

# HVT route_type codes → category (priority order for classification)
ROUTE_TYPE_CATEGORY = {
    "401": "subway",        # Tunnelbana
    "100": "commuter_rail", # Pendeltåg
    "900": "tram",          # Spårväg
    "700": "bus",
    "1000": "ferry",
}
CATEGORY_PRIORITY = ["subway", "commuter_rail", "tram", "bus", "ferry"]


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6_371_000.0
    a1, a2 = math.radians(lat1), math.radians(lat2)
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    h = math.sin(dlat / 2) ** 2 + math.cos(a1) * math.cos(a2) * math.sin(dlon / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(h), math.sqrt(max(0.0, 1.0 - h)))


def load_gtfs(zip_path: Path) -> tuple[dict[str, dict], dict[str, set[str]]]:
    """
    Returns:
      stops: {stop_id: {lat, lon, name}}
      stop_categories: {stop_id: set of category strings}
    """
    with zipfile.ZipFile(zip_path) as z:
        stops_raw = z.read("stops.txt").decode("utf-8-sig")
        routes_raw = z.read("routes.txt").decode("utf-8-sig")
        trips_raw = z.read("trips.txt").decode("utf-8-sig")
        stop_times_raw = z.read("stop_times.txt").decode("utf-8-sig")

    # Parse routes → route_id: category
    route_category: dict[str, str] = {}
    for row in csv.DictReader(io.StringIO(routes_raw)):
        cat = ROUTE_TYPE_CATEGORY.get(row["route_type"])
        if cat:
            route_category[row["route_id"]] = cat

    # Parse trips → trip_id: route_id
    trip_route: dict[str, str] = {}
    for row in csv.DictReader(io.StringIO(trips_raw)):
        if row["route_id"] in route_category:
            trip_route[row["trip_id"]] = row["route_id"]

    # Parse stop_times → stop_id: set of categories
    stop_categories: dict[str, set[str]] = {}
    for row in csv.DictReader(io.StringIO(stop_times_raw)):
        route_id = trip_route.get(row["trip_id"])
        if route_id is None:
            continue
        cat = route_category[route_id]
        sid = row["stop_id"]
        if sid not in stop_categories:
            stop_categories[sid] = set()
        stop_categories[sid].add(cat)

    # Parse stops
    stops: dict[str, dict] = {}
    for row in csv.DictReader(io.StringIO(stops_raw)):
        sid = row["stop_id"]
        try:
            stops[sid] = {
                "lat": float(row["stop_lat"]),
                "lon": float(row["stop_lon"]),
                "name": row.get("stop_name", ""),
            }
        except (ValueError, KeyError):
            continue

    print(f"stops={len(stops)} with_category={len(stop_categories)}")
    cat_counts = {}
    for cats in stop_categories.values():
        for c in cats:
            cat_counts[c] = cat_counts.get(c, 0) + 1
    print(f"stops by category: {cat_counts}")

    return stops, stop_categories


def classify_zone(lat: float, lon: float, stops: dict, stop_categories: dict) -> dict:
    best_by_cat: dict[str, tuple[float, str]] = {}  # cat → (dist_m, name)
    overall_nearest: tuple[float, str, str] = (float("inf"), "", "none")

    for sid, stop in stops.items():
        dist = haversine_m(lat, lon, stop["lat"], stop["lon"])
        if dist > MAX_WALK_M:
            continue
        cats = stop_categories.get(sid, {"bus"})
        name = stop["name"]
        for cat in cats:
            if cat not in best_by_cat or dist < best_by_cat[cat][0]:
                best_by_cat[cat] = (dist, name)
        if dist < overall_nearest[0]:
            # Pick best category for this stop
            best_cat = next((c for c in CATEGORY_PRIORITY if c in cats), "bus")
            overall_nearest = (dist, name, best_cat)

    # Determine transit type by priority
    transit_type = "none"
    for cat in CATEGORY_PRIORITY:
        if cat in best_by_cat:
            transit_type = cat
            break

    nearest_dist_m = int(round(overall_nearest[0])) if overall_nearest[0] < float("inf") else None
    nearest_name = overall_nearest[1] if nearest_dist_m is not None else ""
    nearest_walk_min = None
    if nearest_dist_m is not None:
        nearest_walk_min = min(15, max(1, int(round((nearest_dist_m / 1000) / WALK_SPEED_KMH * 60))))

    return {
        "transit_type": transit_type,
        "nearest_station_name": nearest_name,
        "nearest_station_walk_min": nearest_walk_min if nearest_walk_min is not None else "",
        "nearest_station_dist_m": nearest_dist_m if nearest_dist_m is not None else "",
    }


def main() -> None:
    print("Loading GTFS...")
    stops, stop_categories = load_gtfs(GTFS_ZIP)

    print("Loading DeSO zones...")
    rows = list(csv.DictReader(IN_CSV.open("r", encoding="utf-8")))

    print(f"Classifying {len(rows)} zones...")
    out_rows = []
    for row in rows:
        deso_id = row["id"]
        lat, lon = float(row["lat"]), float(row["lon"])
        result = classify_zone(lat, lon, stops, stop_categories)
        out_rows.append({"desokod": deso_id, **result})

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "desokod", "transit_type", "nearest_station_name",
            "nearest_station_walk_min", "nearest_station_dist_m",
        ])
        writer.writeheader()
        writer.writerows(out_rows)

    from collections import Counter
    type_counts = Counter(r["transit_type"] for r in out_rows)
    print(f"\nwrote {OUT_CSV}")
    print(f"transit type distribution: {dict(type_counts)}")


if __name__ == "__main__":
    main()
