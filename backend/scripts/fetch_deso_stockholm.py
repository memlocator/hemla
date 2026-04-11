from __future__ import annotations

import csv
import json
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

WFS_URL = "https://geodata.scb.se/geoserver/stat/wfs"
PX_BASE = "https://api.scb.se/OV0104/v1/doris/sv/ssd"


def http_get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=180) as r:
        return json.loads(r.read().decode())


def http_post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=300) as r:
        return json.loads(r.read().decode())


def center_from_geometry(geom: dict) -> tuple[float, float]:
    coords = []

    def walk(node):
        if isinstance(node, list):
            if node and isinstance(node[0], (int, float)) and len(node) >= 2:
                coords.append((float(node[0]), float(node[1])))
            else:
                for x in node:
                    walk(x)

    walk(geom.get("coordinates", []))
    if not coords:
        return 0.0, 0.0

    xs = [c[0] for c in coords]
    ys = [c[1] for c in coords]
    return (sum(xs) / len(xs), sum(ys) / len(ys))


def fetch_stockholm_deso_geojson() -> dict:
    params = {
        "service": "WFS",
        "request": "GetFeature",
        "version": "1.1.0",
        "typeName": "stat:DeSO_2025",
        "outputFormat": "application/json",
        "srsName": "EPSG:4326",
        "CQL_FILTER": "lanskod='01'",
    }
    url = f"{WFS_URL}?{urllib.parse.urlencode(params)}"
    return http_get_json(url)


def fetch_income(rows_region: list[str], year: str = "2024") -> dict[str, float]:
    url = f"{PX_BASE}/START/HE/HE0110/HE0110I/Tab2InkDesoRegso"
    payload = {
        "query": [
            {"code": "Region", "selection": {"filter": "item", "values": rows_region}},
            {"code": "Inkomstkomponenter", "selection": {"filter": "item", "values": ["240"]}},  # nettoinkomst
            {"code": "Kon", "selection": {"filter": "item", "values": ["1+2"]}},
            {"code": "ContentsCode", "selection": {"filter": "item", "values": ["000008A4"]}},  # mean
            {"code": "Tid", "selection": {"filter": "item", "values": [year]}},
        ],
        "response": {"format": "json-stat2"},
    }
    data = http_post_json(url, payload)
    out: dict[str, float] = {}
    # json-stat2 shape: dataset dimension/value arrays are verbose; easier to use endpoint with plain json
    # fallback plain json endpoint format:
    payload["response"] = {"format": "json"}
    data = http_post_json(url, payload)
    for row in data.get("data", []):
        key = row.get("key", [])
        if not key:
            continue
        region_code = key[0]
        values = row.get("values", [])
        if not values:
            continue
        try:
            out[region_code] = float(values[0]) * 1000.0  # tkr -> kr
        except Exception:
            continue
    return out


def fetch_unemployment_rate(rows_region: list[str], year: str = "2024") -> dict[str, float]:
    url = f"{PX_BASE}/START/AM/AM0210/AM0210G/ArRegDesoStatusN"

    def fetch(contents_code: str) -> dict[str, float]:
        payload = {
            "query": [
                {"code": "Region", "selection": {"filter": "item", "values": rows_region}},
                {"code": "Kon", "selection": {"filter": "item", "values": ["1+2"]}},
                {"code": "Alder", "selection": {"filter": "item", "values": ["16-64"]}},
                {"code": "ContentsCode", "selection": {"filter": "item", "values": [contents_code]}},
                {"code": "Tid", "selection": {"filter": "item", "values": [year]}},
            ],
            "response": {"format": "json"},
        }
        d = http_post_json(url, payload)
        out: dict[str, float] = {}
        for row in d.get("data", []):
            key = row.get("key", [])
            if not key:
                continue
            vals = row.get("values", [])
            if not vals:
                continue
            try:
                out[key[0]] = float(vals[0])
            except Exception:
                continue
        return out

    unemployed = fetch("0000089W")
    labour_force = fetch("0000089V")

    rate: dict[str, float] = {}
    for r in rows_region:
        lf = labour_force.get(r, 0.0)
        un = unemployed.get(r, 0.0)
        if lf <= 0:
            continue
        rate[r] = (un / lf) * 100.0
    return rate


def main() -> None:
    geo = fetch_stockholm_deso_geojson()
    (RAW / "deso_2025_stockholm.geojson").write_text(json.dumps(geo, ensure_ascii=False), encoding="utf-8")

    features = geo.get("features", [])
    regions = [f["properties"]["desokod"] + "_DeSO2025" for f in features]

    income = fetch_income(regions)
    unemp = fetch_unemployment_rate(regions)

    out_csv = RAW / "deso_stockholm_indicators_2024.csv"
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "desokod",
            "region_code",
            "municipality_code",
            "municipality_name",
            "lat",
            "lon",
            "median_income_sek_proxy",
            "unemployment_rate_pct",
        ])

        for feat in features:
            p = feat.get("properties", {})
            deso = p.get("desokod", "")
            region = f"{deso}_DeSO2025"
            lon, lat = center_from_geometry(feat.get("geometry", {}))
            w.writerow([
                deso,
                region,
                p.get("kommunkod", ""),
                p.get("kommunnamn", ""),
                round(lat, 6),
                round(lon, 6),
                round(income.get(region, 0.0), 2),
                round(unemp.get(region, 0.0), 4),
            ])

    print(f"wrote {out_csv}")
    print(f"features={len(features)} income={len(income)} unemployment={len(unemp)}")


if __name__ == "__main__":
    main()
