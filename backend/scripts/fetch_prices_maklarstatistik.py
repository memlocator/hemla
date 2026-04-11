"""
Municipality-level bostadsrätt price per sqm from Svensk Mäklarstatistik.

Source: https://www.maklarstatistik.se/omrade/riket/stockholms-lan/
Data: 3-month rolling average, fetched 2026-03-05.
Coverage: All 26 municipalities in Stockholm County.
License: Public statistics, freely published by Svensk Mäklarstatistik.

This script writes a CSV that build_hemla_areas_from_deso.py joins into the
main areas CSV, assigning each DeSO zone the price/sqm of its municipality.
Quality flag: "real" (published aggregate from real transactions).

Output:
    backend/data/raw/municipality_prices_brf.csv
    Columns: municipality_name, avg_price_sek_per_sqm, source, fetched_date
"""
from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

OUT_CSV = RAW / "municipality_prices_brf.csv"

# Bostadsrätt price per sqm (SEK/kvm), 3-month rolling average.
# Source: Svensk Mäklarstatistik, https://www.maklarstatistik.se/omrade/riket/stockholms-lan/
# Fetched: 2026-03-05
PRICES: dict[str, int] = {
    "Botkyrka": 28894,
    "Danderyd": 62056,
    "Ekerö": 49231,
    "Haninge": 31888,
    "Huddinge": 38406,
    "Järfälla": 33169,
    "Lidingö": 62582,
    "Nacka": 58476,
    "Norrtälje": 28862,
    "Nykvarn": 32788,
    "Nynäshamn": 24594,
    "Salem": 29078,
    "Sigtuna": 26583,
    "Sollentuna": 40647,
    "Solna": 63037,
    "Stockholm": 87246,
    "Sundbyberg": 64854,
    "Södertälje": 24935,
    "Tyresö": 37544,
    "Täby": 48033,
    "Upplands Väsby": 31294,
    "Upplands-Bro": 29848,
    "Vallentuna": 32693,
    "Vaxholm": 44904,
    "Värmdö": 44547,
    "Österåker": 36454,
}

FETCHED_DATE = "2026-03-05"


def main() -> None:
    rows = [
        {
            "municipality_name": name,
            "avg_price_sek_per_sqm": price,
            "source": "real",
            "fetched_date": FETCHED_DATE,
        }
        for name, price in sorted(PRICES.items())
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["municipality_name", "avg_price_sek_per_sqm", "source", "fetched_date"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"wrote {OUT_CSV} ({len(rows)} municipalities)")
    print(f"price range: {min(PRICES.values()):,} – {max(PRICES.values()):,} SEK/sqm")


if __name__ == "__main__":
    main()
