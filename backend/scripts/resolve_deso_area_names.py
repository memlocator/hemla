from __future__ import annotations

import csv
import json
import math
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
IN_CSV = RAW / "deso_stockholm_indicators_2024.csv"
OUT_CSV = RAW / "deso_area_names.csv"
STATE_JSON = RAW / "deso_area_names.state.json"

NOMINATIM_URL = "https://nominatim.openstreetmap.org/reverse"
USER_AGENT = "Hemla/0.5.0 (deso-name-resolver)"
DELAY_SEC = float(os.getenv("HEMLA_NOMINATIM_DELAY_SEC", "1.05"))
CLUSTER_RADIUS_KM = float(os.getenv("HEMLA_NAME_CLUSTER_RADIUS_KM", "1.5"))

NAME_KEYS = [
    "neighbourhood",
    "suburb",
    "city_district",
    "quarter",
    "residential",
    "town",
    "village",
    "hamlet",
]


def load_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_state(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    out: dict[str, dict[str, str]] = {}
    for k, v in raw.items():
        if isinstance(v, dict):
            out[k.upper()] = {
                "area_name": str(v.get("area_name", "")).strip(),
                "source": str(v.get("source", "")).strip(),
            }
    return out


def save_state(path: Path, state: dict[str, dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)


def pick_name(payload: dict) -> tuple[str, str]:
    address = payload.get("address", {}) if isinstance(payload, dict) else {}
    for key in NAME_KEYS:
        value = address.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip(), key
    display = payload.get("display_name") if isinstance(payload, dict) else None
    if isinstance(display, str) and display.strip():
        return display.split(",")[0].strip(), "display_name"
    return "", "none"


def reverse_name(lat: float, lon: float) -> tuple[str, str]:
    params = {
        "format": "jsonv2",
        "lat": str(lat),
        "lon": str(lon),
        "zoom": "15",
        "addressdetails": "1",
    }
    url = f"{NOMINATIM_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "sv,en"},
    )
    with urllib.request.urlopen(req, timeout=12) as res:
        payload = json.loads(res.read().decode("utf-8"))
    return pick_name(payload)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    a1 = math.radians(lat1)
    a2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    h = math.sin(dlat / 2) ** 2 + math.cos(a1) * math.cos(a2) * math.sin(dlon / 2) ** 2
    return r * (2 * math.atan2(math.sqrt(h), math.sqrt(max(0.0, 1.0 - h))))


def build_clusters(rows: list[dict[str, str]], radius_km: float) -> list[dict]:
    clusters: list[dict] = []
    for row in rows:
        lat = float(row["lat"])
        lon = float(row["lon"])
        municipality = row["municipality_name"].strip().lower()
        matched = None
        for cluster in clusters:
            if cluster["municipality"] != municipality:
                continue
            if haversine_km(lat, lon, cluster["lat"], cluster["lon"]) <= radius_km:
                matched = cluster
                break

        if matched is None:
            clusters.append(
                {
                    "municipality": municipality,
                    "lat": lat,
                    "lon": lon,
                    "members": [row],
                }
            )
            continue

        matched["members"].append(row)
        n = len(matched["members"])
        matched["lat"] = ((matched["lat"] * (n - 1)) + lat) / n
        matched["lon"] = ((matched["lon"] * (n - 1)) + lon) / n

    return clusters


def main() -> None:
    rows = load_rows(IN_CSV)
    state = load_state(STATE_JSON)

    todo = [r for r in rows if r["desokod"].strip().upper() not in state]
    clusters = build_clusters(todo, radius_km=CLUSTER_RADIUS_KM)
    print(f"rows={len(rows)} cached={len(state)} todo={len(todo)} clusters={len(clusters)} radius_km={CLUSTER_RADIUS_KM}")

    for idx, cluster in enumerate(clusters, start=1):
        try:
            name, source = reverse_name(cluster["lat"], cluster["lon"])
            if not name:
                source = "fallback"
        except Exception:
            name, source = "", "error-fallback"

        for member in cluster["members"]:
            code = member["desokod"].strip().upper()
            fallback = f"DeSO {code}"
            state[code] = {"area_name": name or fallback, "source": source}

        if idx % 20 == 0 or idx == len(clusters):
            save_state(STATE_JSON, state)
            print(f"resolved clusters {idx}/{len(clusters)}")
        time.sleep(DELAY_SEC)

    save_state(STATE_JSON, state)

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["desokod", "area_name", "source"])
        writer.writeheader()
        for code in sorted(state.keys()):
            writer.writerow(
                {
                    "desokod": code,
                    "area_name": state[code]["area_name"],
                    "source": state[code]["source"],
                }
            )

    named = sum(1 for v in state.values() if v.get("area_name", "").strip() and not v["area_name"].startswith("DeSO "))
    print(f"wrote {OUT_CSV} rows={len(state)} named={named}")


if __name__ == "__main__":
    main()
