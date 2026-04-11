from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
IN_CSV = RAW / "deso_stockholm_indicators_2024.csv"
NAMES_CSV = RAW / "deso_area_names.csv"
COMMUTE_CSV = RAW / "deso_commute_tcentralen.csv"
PRICES_CSV = RAW / "municipality_prices_brf.csv"
TRANSIT_CSV = RAW / "deso_transit_type.csv"
OUT_CSV = ROOT / "data" / "deso_stockholm_areas.csv"


def load_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_name_map(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    rows = load_rows(path)
    out: dict[str, str] = {}
    for row in rows:
        code = (row.get("desokod") or "").strip().upper()
        name = (row.get("area_name") or "").strip()
        if code and name:
            out[code] = name
    return out


def load_commute_map(path: Path) -> dict[str, dict[str, str]]:
    """Returns {deso_id: {commute_min, source}} from the commute CSV."""
    if not path.exists():
        return {}
    out: dict[str, dict[str, str]] = {}
    for row in load_rows(path):
        deso_id = (row.get("desokod") or "").strip()
        minutes = (row.get("commute_min") or "").strip()
        source = (row.get("source") or "missing").strip()
        if deso_id and minutes:
            out[deso_id] = {"commute_min": minutes, "source": source}
    return out


def load_transit_map(path: Path) -> dict[str, dict[str, str]]:
    """Returns {deso_id: {transit_type, nearest_station_name, nearest_station_walk_min}} from transit CSV."""
    if not path.exists():
        return {}
    out: dict[str, dict[str, str]] = {}
    for row in load_rows(path):
        deso_id = (row.get("desokod") or "").strip()
        if deso_id:
            out[deso_id] = {
                "transit_type": row.get("transit_type", ""),
                "nearest_station_name": row.get("nearest_station_name", ""),
                "nearest_station_walk_min": row.get("nearest_station_walk_min", ""),
            }
    return out


def load_price_map(path: Path) -> dict[str, dict[str, str]]:
    """Returns {municipality_name_lower: {avg_price_sek_per_sqm, source}} from prices CSV."""
    if not path.exists():
        return {}
    out: dict[str, dict[str, str]] = {}
    for row in load_rows(path):
        name = (row.get("municipality_name") or "").strip().lower()
        price = (row.get("avg_price_sek_per_sqm") or "").strip()
        source = (row.get("source") or "missing").strip()
        if name and price:
            out[name] = {"avg_price_sek_per_sqm": price, "source": source}
    return out


def main() -> None:
    indicators = load_rows(IN_CSV)
    name_map = load_name_map(NAMES_CSV)
    commute_map = load_commute_map(COMMUTE_CSV)
    price_map = load_price_map(PRICES_CSV)
    transit_map = load_transit_map(TRANSIT_CSV)

    has_commute = bool(commute_map)
    has_prices = bool(price_map)
    has_transit = bool(transit_map)

    out_fields = [
        "id",
        "name",
        "municipality",
        "lat",
        "lon",
        "median_income_sek",
        "unemployment_rate_pct",
    ]
    if has_commute:
        out_fields += ["sl_commute_to_tcentralen_min", "commute_source"]
    if has_prices:
        out_fields += ["avg_price_sek_per_sqm", "price_source"]
    if has_transit:
        out_fields += ["transit_type", "nearest_station_name", "nearest_station_walk_min"]

    out_rows: list[dict[str, str]] = []
    for row in indicators:
        deso = row["desokod"].strip().upper()
        deso_id = f"deso_{deso.lower()}"
        area_name = name_map.get(deso) or f"DeSO {deso}"
        municipality = row["municipality_name"].strip()
        out_row: dict[str, str] = {
            "id": deso_id,
            "name": area_name,
            "municipality": municipality,
            "lat": f"{float(row['lat']):.6f}",
            "lon": f"{float(row['lon']):.6f}",
            "median_income_sek": str(int(round(float(row["median_income_sek_proxy"])))),
            "unemployment_rate_pct": f"{float(row['unemployment_rate_pct']):.4f}",
        }
        if has_commute:
            commute = commute_map.get(deso_id, {})
            out_row["sl_commute_to_tcentralen_min"] = commute.get("commute_min", "")
            out_row["commute_source"] = commute.get("source", "missing")
        if has_prices:
            price = price_map.get(municipality.lower(), {})
            out_row["avg_price_sek_per_sqm"] = price.get("avg_price_sek_per_sqm", "")
            out_row["price_source"] = price.get("source", "missing")
        if has_transit:
            transit = transit_map.get(deso_id, {})
            out_row["transit_type"] = transit.get("transit_type", "")
            out_row["nearest_station_name"] = transit.get("nearest_station_name", "")
            out_row["nearest_station_walk_min"] = transit.get("nearest_station_walk_min", "")
        out_rows.append(out_row)

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(out_rows)

    named = sum(1 for r in out_rows if not r["name"].startswith("DeSO "))
    with_commute = sum(1 for r in out_rows if r.get("sl_commute_to_tcentralen_min", ""))
    with_price = sum(1 for r in out_rows if r.get("avg_price_sek_per_sqm", ""))
    with_subway = sum(1 for r in out_rows if r.get("transit_type") == "subway")
    print(f"wrote {OUT_CSV} rows={len(out_rows)} named={named} with_commute={with_commute} with_price={with_price} subway_zones={with_subway}")


if __name__ == "__main__":
    main()
