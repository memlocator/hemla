"""
Fetch commute times from each DeSO centroid cluster to T-Centralen using ResRobot v2.1.

Strategy:
- Cluster DeSO zones within 1.5 km, municipality-bounded (reduces ~1362 zones → ~336 clusters)
- Query one ResRobot trip per cluster centroid → T-Centralen
- Propagate cluster travel time to all member DeSO zones
- Cache results to avoid re-fetching; resume-safe via state JSON

Usage:
    python fetch_commute_resrobot.py [--dry-run] [--radius-km 1.5] [--delay-sec 0.5]

Output:
    backend/data/raw/deso_commute_tcentralen.csv
    Columns: desokod, commute_min, cluster_id, source (real|fallback)
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

IN_CSV = ROOT / "data" / "deso_stockholm_areas.csv"
OUT_CSV = RAW / "deso_commute_tcentralen.csv"
STATE_JSON = RAW / "deso_commute_tcentralen.state.json"

RESROBOT_URL = "https://api.resrobot.se/v2.1/trip"
API_KEY = os.environ.get("RESROBOT_API_KEY", "").strip()

# T-Centralen
DEST_LAT = 59.3303
DEST_LON = 18.0586

# Trip query: next Tuesday 08:15 (representative morning peak)
# Computed at import time so it's always a near-future date.
import datetime as _dt

def _next_weekday(weekday: int = 1) -> str:
    """Return next occurrence of weekday (0=Mon..6=Sun) as YYYY-MM-DD."""
    today = _dt.date.today()
    days_ahead = (weekday - today.weekday()) % 7 or 7
    return (today + _dt.timedelta(days=days_ahead)).strftime("%Y-%m-%d")

QUERY_DATE = _next_weekday(1)  # Tuesday
QUERY_TIME = "08:15"


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    a1, a2 = math.radians(lat1), math.radians(lat2)
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    h = math.sin(dlat / 2) ** 2 + math.cos(a1) * math.cos(a2) * math.sin(dlon / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(h), math.sqrt(max(0.0, 1.0 - h)))


def build_clusters(rows: list[dict[str, str]], radius_km: float) -> list[dict]:
    clusters: list[dict] = []
    for row in rows:
        lat = float(row["lat"])
        lon = float(row["lon"])
        mun = row["municipality"].strip().lower()
        matched = None
        for c in clusters:
            if c["municipality"] != mun:
                continue
            if haversine_km(lat, lon, c["lat"], c["lon"]) <= radius_km:
                matched = c
                break
        if matched is None:
            clusters.append({
                "id": len(clusters),
                "municipality": mun,
                "lat": lat,
                "lon": lon,
                "members": [row["id"]],
            })
        else:
            n = len(matched["members"]) + 1
            matched["lat"] = (matched["lat"] * (n - 1) + lat) / n
            matched["lon"] = (matched["lon"] * (n - 1) + lon) / n
            matched["members"].append(row["id"])
    return clusters


def parse_iso_duration(duration: str) -> int | None:
    """Parse ISO 8601 duration like PT29M or PT1H5M to minutes."""
    m = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?", duration or "")
    if not m:
        return None
    hours = int(m.group(1) or 0)
    mins = int(m.group(2) or 0)
    total = hours * 60 + mins
    return total if total > 0 else None


def fetch_trip_minutes(lat: float, lon: float, dry_run: bool) -> int | None:
    if dry_run:
        # Geometric fallback estimate for dry run
        dist = haversine_km(lat, lon, DEST_LAT, DEST_LON)
        return max(6, int(round(dist * 3.5 + 5)))

    params = {
        "originCoordLat": f"{lat:.6f}",
        "originCoordLong": f"{lon:.6f}",
        "destCoordLat": f"{DEST_LAT:.6f}",
        "destCoordLong": f"{DEST_LON:.6f}",
        "date": QUERY_DATE,
        "time": QUERY_TIME,
        "format": "json",
        "numF": "3",
        "accessId": API_KEY,
    }
    url = f"{RESROBOT_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as res:
            data = json.loads(res.read().decode("utf-8"))
    except Exception as e:
        print(f"    request error: {e}")
        return None

    trips = data.get("Trip", [])
    if not trips:
        return None

    durations: list[int] = []
    for trip in trips:
        d = parse_iso_duration(trip.get("duration", ""))
        if d is not None:
            durations.append(d)

    if not durations:
        return None

    # Use median of returned trips
    durations.sort()
    return durations[len(durations) // 2]


def load_state(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_state(path: Path, state: dict) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Use geometric estimate instead of API calls")
    parser.add_argument("--radius-km", type=float, default=1.5)
    parser.add_argument("--delay-sec", type=float, default=0.6)
    args = parser.parse_args()

    if not args.dry_run and not API_KEY:
        raise SystemExit("RESROBOT_API_KEY is required unless --dry-run is used.")

    rows = list(csv.DictReader(IN_CSV.open("r", encoding="utf-8")))
    clusters = build_clusters(rows, radius_km=args.radius_km)

    state = load_state(STATE_JSON)
    done_ids = set(state.keys())
    todo = [c for c in clusters if str(c["id"]) not in done_ids]

    print(f"zones={len(rows)} clusters={len(clusters)} done={len(done_ids)} todo={len(todo)} dry_run={args.dry_run}")

    for idx, cluster in enumerate(todo, start=1):
        cid = str(cluster["id"])
        minutes = fetch_trip_minutes(cluster["lat"], cluster["lon"], dry_run=args.dry_run)
        source = "real" if not args.dry_run else "geometric"
        if minutes is None:
            # Mark as missing — will be interpolated from nearest real cluster in post-processing
            minutes = None
            source = "missing"

        state[cid] = {
            "cluster_lat": cluster["lat"],
            "cluster_lon": cluster["lon"],
            "municipality": cluster["municipality"],
            "commute_min": minutes,
            "source": source,
            "members": cluster["members"],
        }

        print(f"  [{idx}/{len(todo)}] cluster {cid} ({cluster['municipality']}) lat={cluster['lat']:.4f} lon={cluster['lon']:.4f} → {minutes} min [{source}]")

        if idx % 20 == 0 or idx == len(todo):
            save_state(STATE_JSON, state)

        if not args.dry_run:
            time.sleep(args.delay_sec)

    save_state(STATE_JSON, state)

    # Build member → commute_min lookup; interpolate missing from nearest real cluster
    real_clusters = [
        (float(c["cluster_lat"]), float(c["cluster_lon"]), int(c["commute_min"]))
        for c in state.values()
        if c.get("source") == "real" and c.get("commute_min") is not None
    ]

    def nearest_real_commute(lat: float, lon: float) -> tuple[int, str]:
        if not real_clusters:
            return None, "missing"
        best_min, best_dist = None, float("inf")
        for rlat, rlon, rmin in real_clusters:
            d = haversine_km(lat, lon, rlat, rlon)
            if d < best_dist:
                best_dist, best_min = d, rmin
        # Only interpolate if the nearest real cluster is within 30 km
        if best_dist <= 30.0:
            return best_min, "interpolated"
        return None, "missing"

    deso_commute: dict[str, dict] = {}
    for cid, c in state.items():
        raw_minutes = c.get("commute_min")
        source = c.get("source", "missing")
        if source in ("missing",) or raw_minutes is None:
            minutes, source = nearest_real_commute(float(c["cluster_lat"]), float(c["cluster_lon"]))
        else:
            minutes = int(raw_minutes)
        for deso_id in c.get("members", []):
            deso_commute[deso_id] = {
                "commute_min": minutes,
                "cluster_id": cid,
                "source": source,
            }

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["desokod", "commute_min", "cluster_id", "source"])
        writer.writeheader()
        for row in rows:
            deso_id = row["id"]
            entry = deso_commute.get(deso_id)
            if entry and entry["commute_min"] is not None:
                writer.writerow({
                    "desokod": deso_id,
                    "commute_min": entry["commute_min"],
                    "cluster_id": entry["cluster_id"],
                    "source": entry["source"],
                })
            else:
                writer.writerow({
                    "desokod": deso_id,
                    "commute_min": "",
                    "cluster_id": "",
                    "source": "missing",
                })

    counts = {"real": 0, "interpolated": 0, "missing": 0}
    for v in deso_commute.values():
        counts[v["source"]] = counts.get(v["source"], 0) + 1
    print(f"\nwrote {OUT_CSV}")
    print(f"zones: {len(deso_commute)}/{len(rows)} — real={counts['real']} interpolated={counts['interpolated']} missing={counts['missing']}")


if __name__ == "__main__":
    main()
