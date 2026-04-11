from __future__ import annotations

import asyncio
import csv
import math
import os
import time
from pathlib import Path
from typing import Dict, List, Literal

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

try:
    from transit import TransitEngine
    _TRANSIT_ENGINE_AVAILABLE = True
except ImportError:
    _TRANSIT_ENGINE_AVAILABLE = False


class Coordinates(BaseModel):
    lat: float
    lon: float


class AreaMetrics(BaseModel):
    avg_price_sek_per_sqm: int | None = Field(default=None, description="Average sale price per sqm")
    monthly_rent_2br_sek: int | None = Field(default=None, description="Monthly rent for 2BR")
    sl_commute_to_tcentralen_min: int | None = Field(default=None, description="Typical SL commute time to T-Centralen")
    sl_departures_per_hour_peak: int | None = Field(default=None, description="Peak departures per hour")
    nearest_station_walk_min: int | None = Field(default=None, description="Walking time to nearest SL station")
    schools_score: float | None = Field(default=None, ge=0, le=10)
    green_access_score: float | None = Field(default=None, ge=0, le=10)
    flood_risk_score: float | None = Field(default=None, ge=0, le=10, description="10=high risk")
    noise_score: float | None = Field(default=None, ge=0, le=10, description="10=very noisy")
    median_income_sek: int = Field(description="Official DeSO-level annual net income proxy (SCB)")
    unemployment_rate_pct: float = Field(description="Official DeSO-level unemployment rate (SCB)")
    crime_rate_per_1000: float | None = Field(default=None, description="Crime incidents per 1000 residents")
    pm25_ugm3: float | None = Field(default=None, description="PM2.5 concentration")
    healthcare_access_score: float | None = Field(default=None, ge=0, le=10, description="Primary care and emergency access")
    bikeability_score: float | None = Field(default=None, ge=0, le=10, description="Bike infra + terrain friendliness")
    broadband_coverage_pct: float | None = Field(default=None, description="Household access to >=1 Gbit/s")
    transit_type: str | None = Field(default=None, description="Best transit type within 800m: subway, commuter_rail, tram, bus, none")
    nearest_station_name: str | None = Field(default=None, description="Name of nearest SL station within 800m")


class Area(BaseModel):
    id: str
    name: str
    municipality: str
    coordinates: Coordinates
    metrics: AreaMetrics


class AreaResult(BaseModel):
    area: Area
    value_score: float
    breakdown: Dict[str, float | None]


class AreaCollection(BaseModel):
    total: int
    items: List[AreaResult]


class SourceInfo(BaseModel):
    id: str
    name: str
    category: str
    url: str
    notes: str


class ConnectionEdge(BaseModel):
    from_area_id: str
    to_area_id: str
    connection_score: float
    estimated_trip_min: int
    distance_km: float


class ConnectionGraph(BaseModel):
    total_edges: int
    edges: List[ConnectionEdge]


class PeerComparison(BaseModel):
    area_id: str
    name: str
    municipality: str
    similarity: float
    value_score: float
    price_diff_sek_per_sqm: int
    commute_diff_min: int


class DrilldownResponse(BaseModel):
    area: AreaResult
    metric_percentiles: Dict[str, float]
    peers: List[PeerComparison]
    opportunities: List[str]


class ApartmentListing(BaseModel):
    id: str
    area_id: str
    area_name: str
    municipality: str
    rooms: float
    sqm: int
    monthly_rent_sek: int
    estimated_sale_price_sek: int
    price_per_sqm_sek: int
    commute_times_min: Dict[str, int]
    features: List[str]
    fit_score: float
    destination_label: str | None = None
    commute_to_destination_min: int | None = None


class ApartmentCollection(BaseModel):
    total: int
    items: List[ApartmentListing]


class GeocodeCandidate(BaseModel):
    label: str
    lat: float
    lon: float


app = FastAPI(title="Hemla", version="0.4.0")


@app.on_event("startup")
async def _load_transit_engine():
    global TRANSIT_ENGINE
    if not _TRANSIT_ENGINE_AVAILABLE:
        return

    def _load():
        global TRANSIT_ENGINE
        try:
            gtfs_zip = Path(__file__).resolve().parent.parent / "data" / "raw" / "sl_gtfs.zip"
            deso_csv = Path(__file__).resolve().parent.parent / "data" / "deso_stockholm_areas.csv"
            if not gtfs_zip.exists() or not deso_csv.exists():
                print("[transit] GTFS zip or DeSO CSV not found, engine disabled", flush=True)
                return
            engine = TransitEngine.load(gtfs_zip, deso_csv, arrival_hhmm="08:45")
            engine.travel_times_to(59.3303, 18.0586)  # pre-warm T-Centralen
            TRANSIT_ENGINE = engine
            import sys
            print("[transit] Engine ready, serving GTFS commute times.", file=sys.stderr, flush=True)
        except Exception as e:
            import sys
            print(f"[transit] Failed to load: {e}", file=sys.stderr, flush=True)

    import threading
    threading.Thread(target=_load, daemon=True).start()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SOURCES: List[SourceInfo] = [
    SourceInfo(
        id="lantmateriet",
        name="Lantmateriet Open Geodata",
        category="Housing & Land",
        url="https://www.lantmateriet.se/sv/geodata/vara-produkter/oppna-data/",
        notes="Basemaps, addresses, place names, and terrain data for free use.",
    ),
    SourceInfo(
        id="sl",
        name="SL Trafiklab APIs",
        category="Transit",
        url="https://www.trafiklab.se/api/",
        notes="Stop departures, travel plans, and GTFS data for Stockholm transit (API keys may be needed).",
    ),
    SourceInfo(
        id="scb",
        name="SCB Open API",
        category="Socioeconomic",
        url="https://www.scb.se/en/services/open-data-api/",
        notes="Income, population, employment and housing cost indicators.",
    ),
    SourceInfo(
        id="open_meteo",
        name="Open-Meteo",
        category="Environment & Climate",
        url="https://open-meteo.com/",
        notes="Live PM2.5 and precipitation signals used in enrichment.",
    ),
    SourceInfo(
        id="bra",
        name="BRA (Crime Statistics)",
        category="Safety",
        url="https://bra.se/statistik",
        notes="Municipality-level crime indicators.",
    ),
    SourceInfo(
        id="pts",
        name="PTS Broadband Data",
        category="Digital Infrastructure",
        url="https://www.pts.se/sv/bransch/internet/bredbandskartlaggning/",
        notes="Broadband coverage and speed availability at municipal levels.",
    ),
    SourceInfo(
        id="osm",
        name="OpenStreetMap / Overpass",
        category="Amenities",
        url="https://overpass-turbo.eu/",
        notes="Live amenities and cycle infra density used in enrichment.",
    ),
]

# Static baseline, enriched with live sources if `live=true` is passed to API.
# Covers all municipalities in Stockholm County with representative local areas.
STOCKHOLM_AREA_SEEDS: List[Dict[str, float | str]] = [
    {"id": "norrmalm", "name": "Norrmalm", "municipality": "Stockholm", "lat": 59.334, "lon": 18.063, "price": 98000, "commute": 8, "departures": 20, "walk": 3, "green": 6.2, "income": 445000, "unemployment": 3.2, "crime": 61.0, "pm25": 8.0, "broadband": 98.0},
    {"id": "liljeholmen", "name": "Liljeholmen", "municipality": "Stockholm", "lat": 59.311, "lon": 18.022, "price": 86000, "commute": 11, "departures": 16, "walk": 3, "green": 6.9, "income": 418000, "unemployment": 4.0, "crime": 57.0, "pm25": 7.8, "broadband": 96.0},
    {"id": "solna_centrum", "name": "Solna Centrum", "municipality": "Solna", "lat": 59.359, "lon": 18.000, "price": 76000, "commute": 10, "departures": 17, "walk": 4, "green": 6.9, "income": 436000, "unemployment": 3.6, "crime": 46.0, "pm25": 7.4, "broadband": 97.0},
    {"id": "sundbyberg_centrum", "name": "Sundbyberg Centrum", "municipality": "Sundbyberg", "lat": 59.361, "lon": 17.971, "price": 69000, "commute": 13, "departures": 18, "walk": 4, "green": 7.4, "income": 393000, "unemployment": 4.3, "crime": 52.0, "pm25": 7.1, "broadband": 94.0},
    {"id": "jakobsberg", "name": "Jakobsberg", "municipality": "Järfälla", "lat": 59.423, "lon": 17.835, "price": 47000, "commute": 24, "departures": 10, "walk": 7, "green": 7.9, "income": 352000, "unemployment": 5.1, "crime": 49.0, "pm25": 6.8, "broadband": 91.0},
    {"id": "viksjo", "name": "Viksjö", "municipality": "Järfälla", "lat": 59.415, "lon": 17.782, "price": 51000, "commute": 28, "departures": 8, "walk": 9, "green": 8.2, "income": 366000, "unemployment": 4.5, "crime": 42.0, "pm25": 6.4, "broadband": 91.0},
    {"id": "sollentuna_centrum", "name": "Sollentuna Centrum", "municipality": "Sollentuna", "lat": 59.428, "lon": 17.950, "price": 62000, "commute": 18, "departures": 13, "walk": 6, "green": 7.4, "income": 407000, "unemployment": 3.9, "crime": 41.0, "pm25": 6.9, "broadband": 95.0},
    {"id": "haggvik", "name": "Häggvik", "municipality": "Sollentuna", "lat": 59.442, "lon": 17.932, "price": 59000, "commute": 20, "departures": 12, "walk": 7, "green": 7.8, "income": 401000, "unemployment": 3.8, "crime": 39.0, "pm25": 6.7, "broadband": 95.0},
    {"id": "upplands_vasby_centrum", "name": "Upplands Väsby Centrum", "municipality": "Upplands Väsby", "lat": 59.518, "lon": 17.911, "price": 52000, "commute": 25, "departures": 10, "walk": 8, "green": 7.6, "income": 360000, "unemployment": 4.8, "crime": 45.0, "pm25": 6.6, "broadband": 92.0},
    {"id": "rotebro", "name": "Rotebro", "municipality": "Upplands Väsby", "lat": 59.478, "lon": 17.914, "price": 50000, "commute": 23, "departures": 10, "walk": 8, "green": 7.2, "income": 349000, "unemployment": 5.0, "crime": 47.0, "pm25": 6.8, "broadband": 92.0},
    {"id": "kungsangen", "name": "Kungsängen", "municipality": "Upplands-Bro", "lat": 59.478, "lon": 17.752, "price": 42000, "commute": 33, "departures": 7, "walk": 9, "green": 8.3, "income": 352000, "unemployment": 4.6, "crime": 38.0, "pm25": 6.2, "broadband": 89.0},
    {"id": "bro", "name": "Bro", "municipality": "Upplands-Bro", "lat": 59.516, "lon": 17.633, "price": 39500, "commute": 38, "departures": 6, "walk": 11, "green": 8.8, "income": 342000, "unemployment": 4.9, "crime": 35.0, "pm25": 5.9, "broadband": 87.0},
    {"id": "taby_centrum", "name": "Täby Centrum", "municipality": "Täby", "lat": 59.444, "lon": 18.070, "price": 68000, "commute": 22, "departures": 12, "walk": 6, "green": 7.3, "income": 468000, "unemployment": 2.8, "crime": 30.0, "pm25": 6.5, "broadband": 97.0},
    {"id": "nasbypark", "name": "Näsbypark", "municipality": "Täby", "lat": 59.431, "lon": 18.091, "price": 72000, "commute": 27, "departures": 9, "walk": 9, "green": 8.1, "income": 482000, "unemployment": 2.6, "crime": 26.0, "pm25": 6.1, "broadband": 97.0},
    {"id": "vallentuna_centrum", "name": "Vallentuna Centrum", "municipality": "Vallentuna", "lat": 59.534, "lon": 18.078, "price": 45500, "commute": 36, "departures": 7, "walk": 10, "green": 8.4, "income": 427000, "unemployment": 3.0, "crime": 28.0, "pm25": 5.8, "broadband": 90.0},
    {"id": "akersberga", "name": "Åkersberga", "municipality": "Österåker", "lat": 59.478, "lon": 18.299, "price": 43000, "commute": 40, "departures": 7, "walk": 9, "green": 8.8, "income": 375000, "unemployment": 3.7, "crime": 36.0, "pm25": 6.1, "broadband": 88.0, "coastal": 1.0},
    {"id": "vaxholm", "name": "Vaxholm", "municipality": "Vaxholm", "lat": 59.402, "lon": 18.353, "price": 61000, "commute": 47, "departures": 5, "walk": 11, "green": 9.2, "income": 452000, "unemployment": 2.4, "crime": 22.0, "pm25": 5.3, "broadband": 89.0, "coastal": 1.0},
    {"id": "lidingo_centrum", "name": "Lidingö Centrum", "municipality": "Lidingö", "lat": 59.364, "lon": 18.145, "price": 82000, "commute": 23, "departures": 11, "walk": 6, "green": 8.2, "income": 536000, "unemployment": 2.3, "crime": 25.0, "pm25": 6.2, "broadband": 97.0, "coastal": 1.0},
    {"id": "nacka_forum", "name": "Nacka Forum", "municipality": "Nacka", "lat": 59.309, "lon": 18.166, "price": 70000, "commute": 24, "departures": 12, "walk": 7, "green": 7.6, "income": 482000, "unemployment": 2.7, "crime": 32.0, "pm25": 6.4, "broadband": 96.0, "coastal": 1.0},
    {"id": "sickla", "name": "Sickla", "municipality": "Nacka", "lat": 59.306, "lon": 18.121, "price": 84000, "commute": 16, "departures": 15, "walk": 5, "green": 6.8, "income": 498000, "unemployment": 2.5, "crime": 33.0, "pm25": 6.9, "broadband": 96.0},
    {"id": "tyreso_centrum", "name": "Tyresö Centrum", "municipality": "Tyresö", "lat": 59.243, "lon": 18.231, "price": 52000, "commute": 37, "departures": 9, "walk": 9, "green": 8.4, "income": 391000, "unemployment": 3.6, "crime": 31.0, "pm25": 5.9, "broadband": 92.0},
    {"id": "gustavsberg", "name": "Gustavsberg", "municipality": "Värmdö", "lat": 59.325, "lon": 18.389, "price": 56000, "commute": 44, "departures": 7, "walk": 10, "green": 9.1, "income": 409000, "unemployment": 3.2, "crime": 27.0, "pm25": 5.5, "broadband": 90.0, "coastal": 1.0},
    {"id": "hemmesta", "name": "Hemmesta", "municipality": "Värmdö", "lat": 59.339, "lon": 18.485, "price": 51000, "commute": 52, "departures": 5, "walk": 13, "green": 9.4, "income": 401000, "unemployment": 3.4, "crime": 24.0, "pm25": 5.2, "broadband": 88.0, "coastal": 1.0},
    {"id": "haninge_centrum", "name": "Haninge Centrum", "municipality": "Haninge", "lat": 59.168, "lon": 18.137, "price": 45000, "commute": 34, "departures": 10, "walk": 8, "green": 8.1, "income": 334000, "unemployment": 5.8, "crime": 48.0, "pm25": 6.2, "broadband": 89.0},
    {"id": "vega", "name": "Vega", "municipality": "Haninge", "lat": 59.184, "lon": 18.154, "price": 51000, "commute": 31, "departures": 11, "walk": 7, "green": 7.8, "income": 351000, "unemployment": 5.1, "crime": 43.0, "pm25": 6.3, "broadband": 90.0},
    {"id": "nynashamn", "name": "Nynäshamn", "municipality": "Nynäshamn", "lat": 58.903, "lon": 17.950, "price": 31000, "commute": 63, "departures": 4, "walk": 13, "green": 9.0, "income": 318000, "unemployment": 5.9, "crime": 34.0, "pm25": 4.9, "broadband": 84.0, "coastal": 1.0},
    {"id": "huddinge_centrum", "name": "Huddinge Centrum", "municipality": "Huddinge", "lat": 59.236, "lon": 17.983, "price": 56000, "commute": 24, "departures": 12, "walk": 6, "green": 7.7, "income": 365000, "unemployment": 4.8, "crime": 44.0, "pm25": 6.8, "broadband": 93.0},
    {"id": "flemingsberg", "name": "Flemingsberg", "municipality": "Huddinge", "lat": 59.221, "lon": 17.948, "price": 47000, "commute": 25, "departures": 11, "walk": 7, "green": 7.3, "income": 332000, "unemployment": 6.1, "crime": 53.0, "pm25": 6.7, "broadband": 92.0},
    {"id": "botkyrka_hallunda", "name": "Hallunda", "municipality": "Botkyrka", "lat": 59.243, "lon": 17.825, "price": 36000, "commute": 32, "departures": 11, "walk": 7, "green": 7.5, "income": 302000, "unemployment": 7.0, "crime": 58.0, "pm25": 6.6, "broadband": 87.0},
    {"id": "tumba", "name": "Tumba", "municipality": "Botkyrka", "lat": 59.199, "lon": 17.833, "price": 39000, "commute": 34, "departures": 9, "walk": 8, "green": 8.2, "income": 321000, "unemployment": 6.2, "crime": 50.0, "pm25": 6.1, "broadband": 87.0},
    {"id": "salem_centrum", "name": "Salem Centrum", "municipality": "Salem", "lat": 59.201, "lon": 17.748, "price": 42000, "commute": 36, "departures": 8, "walk": 9, "green": 8.5, "income": 360000, "unemployment": 4.4, "crime": 33.0, "pm25": 5.8, "broadband": 90.0},
    {"id": "sodertalje_centrum", "name": "Södertälje Centrum", "municipality": "Södertälje", "lat": 59.195, "lon": 17.628, "price": 32000, "commute": 42, "departures": 8, "walk": 9, "green": 7.1, "income": 307000, "unemployment": 7.2, "crime": 62.0, "pm25": 6.4, "broadband": 88.0},
    {"id": "jarna", "name": "Järna", "municipality": "Södertälje", "lat": 59.091, "lon": 17.567, "price": 28500, "commute": 55, "departures": 5, "walk": 11, "green": 9.1, "income": 319000, "unemployment": 5.3, "crime": 29.0, "pm25": 5.2, "broadband": 83.0},
    {"id": "nykvarn", "name": "Nykvarn", "municipality": "Nykvarn", "lat": 59.178, "lon": 17.433, "price": 30000, "commute": 53, "departures": 5, "walk": 11, "green": 9.0, "income": 347000, "unemployment": 4.8, "crime": 25.0, "pm25": 5.3, "broadband": 82.0},
    {"id": "sigtuna_marsta", "name": "Märsta", "municipality": "Sigtuna", "lat": 59.620, "lon": 17.854, "price": 43000, "commute": 36, "departures": 9, "walk": 8, "green": 7.6, "income": 336000, "unemployment": 5.7, "crime": 46.0, "pm25": 6.0, "broadband": 88.0},
    {"id": "arlanda", "name": "Arlanda Omrade", "municipality": "Sigtuna", "lat": 59.651, "lon": 17.923, "price": 39000, "commute": 34, "departures": 10, "walk": 9, "green": 6.4, "income": 355000, "unemployment": 5.2, "crime": 40.0, "pm25": 7.1, "broadband": 90.0},
    {"id": "danderyd_centrum", "name": "Danderyd Centrum", "municipality": "Danderyd", "lat": 59.398, "lon": 18.042, "price": 83000, "commute": 18, "departures": 12, "walk": 6, "green": 8.0, "income": 571000, "unemployment": 1.8, "crime": 24.0, "pm25": 6.5, "broadband": 98.0},
    {"id": "ekaero_centrum", "name": "Ekerö Centrum", "municipality": "Ekerö", "lat": 59.291, "lon": 17.808, "price": 51000, "commute": 43, "departures": 6, "walk": 10, "green": 9.2, "income": 454000, "unemployment": 2.7, "crime": 23.0, "pm25": 5.6, "broadband": 86.0},
    {"id": "norrtalje", "name": "Norrtälje", "municipality": "Norrtälje", "lat": 59.759, "lon": 18.702, "price": 33000, "commute": 70, "departures": 4, "walk": 13, "green": 9.5, "income": 334000, "unemployment": 4.7, "crime": 27.0, "pm25": 4.8, "broadband": 84.0, "coastal": 1.0},
]


def area_from_seed(seed: Dict[str, float | str]) -> Area:
    def c10(value: float) -> float:
        return max(0.0, min(10.0, value))

    price = int(seed["price"])
    commute = int(seed["commute"])
    departures = int(seed["departures"])
    walk = int(seed["walk"])
    green = float(seed["green"])
    income = int(seed["income"])
    unemployment = float(seed["unemployment"])
    crime = float(seed["crime"])
    pm25 = float(seed["pm25"])
    broadband = float(seed["broadband"])
    coastal = float(seed.get("coastal", 0.0))

    monthly_rent = int(round(max(9000, min(26000, price * 0.22)) / 100.0) * 100)
    schools = c10(5.1 + (income - 320000) / 90000 * 0.9 - unemployment * 0.08)
    flood = c10(1.8 + coastal * 0.8 + max(0.0, green - 8.6) * 0.2)
    noise = c10(2.9 + departures * 0.18 + (10 - green) * 0.12 + max(0, 7.2 - pm25) * 0.1)
    healthcare = c10(5.3 + departures * 0.2 - walk * 0.12)
    bike = c10(4.5 + (12 - walk) * 0.18 + (green - 6.0) * 0.25)

    return Area(
        id=str(seed["id"]),
        name=str(seed["name"]),
        municipality=str(seed["municipality"]),
        coordinates=Coordinates(lat=float(seed["lat"]), lon=float(seed["lon"])),
        metrics=AreaMetrics(
            avg_price_sek_per_sqm=price,
            monthly_rent_2br_sek=monthly_rent,
            sl_commute_to_tcentralen_min=commute,
            sl_departures_per_hour_peak=departures,
            nearest_station_walk_min=walk,
            schools_score=round(schools, 2),
            green_access_score=round(green, 2),
            flood_risk_score=round(flood, 2),
            noise_score=round(noise, 2),
            median_income_sek=income,
            unemployment_rate_pct=round(unemployment, 2),
            crime_rate_per_1000=round(crime, 2),
            pm25_ugm3=round(pm25, 2),
            healthcare_access_score=round(healthcare, 2),
            bikeability_score=round(bike, 2),
            broadband_coverage_pct=round(broadband, 2),
        ),
    )


BASE_AREAS: List[Area] = [area_from_seed(seed) for seed in STOCKHOLM_AREA_SEEDS]
BASE_AREAS_SOURCE = "seed"


def load_real_areas_from_csv(csv_path: Path) -> List[Area]:
    if not csv_path.exists():
        return []

    required = {"id", "name", "municipality", "lat", "lon", "median_income_sek", "unemployment_rate_pct"}

    def parse_int(raw: dict[str, str], key: str) -> int | None:
        value = raw.get(key)
        if value is None or value == "":
            return None
        return int(float(value))

    def parse_float(raw: dict[str, str], key: str) -> float | None:
        value = raw.get(key)
        if value is None or value == "":
            return None
        return float(value)

    rows: List[Area] = []
    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return []
        missing = required - set(reader.fieldnames)
        if missing:
            return []

        for raw in reader:
            rows.append(
                Area(
                    id=raw["id"].strip(),
                    name=raw["name"].strip(),
                    municipality=raw["municipality"].strip(),
                    coordinates=Coordinates(lat=float(raw["lat"]), lon=float(raw["lon"])),
                    metrics=AreaMetrics(
                        avg_price_sek_per_sqm=parse_int(raw, "avg_price_sek_per_sqm"),
                        monthly_rent_2br_sek=parse_int(raw, "monthly_rent_2br_sek"),
                        sl_commute_to_tcentralen_min=parse_int(raw, "sl_commute_to_tcentralen_min"),
                        sl_departures_per_hour_peak=parse_int(raw, "sl_departures_per_hour_peak"),
                        nearest_station_walk_min=parse_int(raw, "nearest_station_walk_min"),
                        schools_score=parse_float(raw, "schools_score"),
                        green_access_score=parse_float(raw, "green_access_score"),
                        flood_risk_score=parse_float(raw, "flood_risk_score"),
                        noise_score=parse_float(raw, "noise_score"),
                        median_income_sek=int(float(raw["median_income_sek"])),
                        unemployment_rate_pct=float(raw["unemployment_rate_pct"]),
                        crime_rate_per_1000=parse_float(raw, "crime_rate_per_1000"),
                        pm25_ugm3=parse_float(raw, "pm25_ugm3"),
                        healthcare_access_score=parse_float(raw, "healthcare_access_score"),
                        bikeability_score=parse_float(raw, "bikeability_score"),
                        broadband_coverage_pct=parse_float(raw, "broadband_coverage_pct"),
                        transit_type=raw.get("transit_type") or None,
                        nearest_station_name=raw.get("nearest_station_name") or None,
                    ),
                )
            )

    return rows


def resolve_base_areas() -> tuple[List[Area], str]:
    configured = os.getenv("HEMLA_AREAS_CSV", "").strip()
    csv_path = Path(configured) if configured else Path(__file__).resolve().parent.parent / "data" / "deso_stockholm_areas.csv"
    real_rows = load_real_areas_from_csv(csv_path)
    if real_rows:
        return real_rows, f"csv:{csv_path}"
    return [], "empty"


BASE_AREAS, BASE_AREAS_SOURCE = resolve_base_areas()

TRANSIT_ENGINE: "TransitEngine | None" = None

ENRICHMENT_CACHE: Dict[str, tuple[float, AreaMetrics]] = {}
ENRICHMENT_TTL_SEC = 6 * 60 * 60
GEOCODE_CACHE: Dict[str, tuple[float, List[GeocodeCandidate]]] = {}
GEOCODE_TTL_SEC = 24 * 60 * 60

PRESET_DESTINATIONS: Dict[str, Coordinates] = {
    "tcentralen": Coordinates(lat=59.3303, lon=18.0586),
    "kista": Coordinates(lat=59.4031, lon=17.9448),
    "karolinska": Coordinates(lat=59.3496, lon=18.0310),
    "sodermalm": Coordinates(lat=59.3143, lon=18.0730),
}

PRESET_DESTINATION_LABELS: Dict[str, str] = {
    "tcentralen": "T-Centralen",
    "kista": "Kista",
    "karolinska": "Karolinska",
    "sodermalm": "Sodermalm",
}


def normalize(value: float, low: float, high: float, reverse: bool = False) -> float:
    if high == low:
        return 0.0
    clamped = max(low, min(high, value))
    scaled = (clamped - low) / (high - low)
    return 1.0 - scaled if reverse else scaled


def clamp_0_10(value: float) -> float:
    return max(0.0, min(10.0, value))


def haversine_km(a: Coordinates, b: Coordinates) -> float:
    radius_km = 6371.0
    lat1 = math.radians(a.lat)
    lat2 = math.radians(b.lat)
    d_lat = math.radians(b.lat - a.lat)
    d_lon = math.radians(b.lon - a.lon)

    sin_dlat = math.sin(d_lat / 2.0)
    sin_dlon = math.sin(d_lon / 2.0)
    h = sin_dlat * sin_dlat + math.cos(lat1) * math.cos(lat2) * sin_dlon * sin_dlon
    return radius_km * (2.0 * math.atan2(math.sqrt(h), math.sqrt(max(0.0, 1.0 - h))))


def estimate_commute_minutes(area: Area, destination: Coordinates) -> int | None:
    baseline = area.metrics.sl_commute_to_tcentralen_min
    if baseline is None:
        return None

    # If GTFS engine is available, use ratio-scaling:
    # scale the real ResRobot T-Centralen time by the GTFS ratio (dest / T-Centralen)
    # This cancels out GTFS systematic bias and gives good relative accuracy.
    if TRANSIT_ENGINE is not None:
        tcen = PRESET_DESTINATIONS["tcentralen"]
        gtfs_tcen = TRANSIT_ENGINE.travel_times_to(tcen.lat, tcen.lon).get(area.id)
        gtfs_dest = TRANSIT_ENGINE.travel_times_to(destination.lat, destination.lon).get(area.id)
        if gtfs_tcen and gtfs_dest and gtfs_tcen > 0:
            ratio = gtfs_dest / gtfs_tcen
            return int(max(1, min(180, round(baseline * ratio))))
        # Fall back to raw GTFS dest time if no T-Centralen reference
        if gtfs_dest:
            return int(max(1, min(180, gtfs_dest)))

    # Geometric fallback (no GTFS)
    center = PRESET_DESTINATIONS["tcentralen"]
    dist_to_center = max(1.2, haversine_km(area.coordinates, center))
    dist_to_destination = haversine_km(area.coordinates, destination)
    min_per_km = baseline / dist_to_center
    estimate = dist_to_destination * min_per_km + 4.0
    return int(max(6, min(180, round(estimate))))


async def resolve_destination(
    destination: str | None = None,
    destination_query: str | None = None,
    destination_lat: float | None = None,
    destination_lon: float | None = None,
) -> tuple[Coordinates, str]:
    coords = PRESET_DESTINATIONS["tcentralen"]
    label = PRESET_DESTINATION_LABELS["tcentralen"]

    if destination_lat is not None or destination_lon is not None:
        if destination_lat is None or destination_lon is None:
            raise HTTPException(status_code=400, detail="Provide both destination_lat and destination_lon")
        return Coordinates(lat=destination_lat, lon=destination_lon), (destination_query.strip() if destination_query else "Custom destination")

    if destination_query and len(destination_query.strip()) < 2:
        return coords, label

    if destination_query:
        candidates = await geocode_destination(destination_query, limit=1)
        if not candidates:
            return coords, label
        return Coordinates(lat=candidates[0].lat, lon=candidates[0].lon), candidates[0].label

    if destination and destination.lower() in PRESET_DESTINATIONS:
        key = destination.lower()
        return PRESET_DESTINATIONS[key], PRESET_DESTINATION_LABELS[key]

    if destination:
        candidates = await geocode_destination(destination, limit=1)
        if candidates:
            return Coordinates(lat=candidates[0].lat, lon=candidates[0].lon), candidates[0].label

    return coords, label


async def geocode_destination(query: str, limit: int = 6) -> List[GeocodeCandidate]:
    key = query.strip().lower()
    if not key:
        return []

    now = time.time()
    cached = GEOCODE_CACHE.get(key)
    if cached and (now - cached[0] < GEOCODE_TTL_SEC):
        return cached[1][:limit]

    timeout = httpx.Timeout(8.0, connect=3.0)
    params = {
        "q": query,
        "format": "jsonv2",
        "limit": min(10, max(1, limit)),
        "countrycodes": "se",
        "addressdetails": 0,
    }
    headers = {
        "User-Agent": "Hemla/0.4.0 (local-dev)",
        "Accept-Language": "sv,en",
    }

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            res = await client.get("https://nominatim.openstreetmap.org/search", params=params, headers=headers)
            res.raise_for_status()
            payload = res.json()
    except Exception:
        return []

    out: List[GeocodeCandidate] = []
    for item in payload:
        lat = item.get("lat")
        lon = item.get("lon")
        label = item.get("display_name")
        if lat is None or lon is None or not label:
            continue
        try:
            out.append(GeocodeCandidate(label=str(label), lat=float(lat), lon=float(lon)))
        except Exception:
            continue

    GEOCODE_CACHE[key] = (now, out)
    return out[:limit]


def build_apartment_listings() -> List[ApartmentListing]:
    templates = [
        {"rooms": 1.0, "sqm": 28, "rent_factor": 0.62, "price_factor": 0.92},
        {"rooms": 1.5, "sqm": 36, "rent_factor": 0.78, "price_factor": 0.96},
        {"rooms": 2.0, "sqm": 46, "rent_factor": 0.93, "price_factor": 0.99},
        {"rooms": 2.5, "sqm": 58, "rent_factor": 1.07, "price_factor": 1.02},
        {"rooms": 3.0, "sqm": 72, "rent_factor": 1.24, "price_factor": 1.04},
    ]

    rows: List[ApartmentListing] = []
    for area in BASE_AREAS:
        if (
            area.metrics.avg_price_sek_per_sqm is None
            or area.metrics.monthly_rent_2br_sek is None
            or area.metrics.sl_commute_to_tcentralen_min is None
        ):
            continue
        for idx, t in enumerate(templates):
            tcentralen_base = estimate_commute_minutes(area, PRESET_DESTINATIONS["tcentralen"])
            kista_base = estimate_commute_minutes(area, PRESET_DESTINATIONS["kista"])
            karolinska_base = estimate_commute_minutes(area, PRESET_DESTINATIONS["karolinska"])
            sodermalm_base = estimate_commute_minutes(area, PRESET_DESTINATIONS["sodermalm"])
            if tcentralen_base is None or kista_base is None or karolinska_base is None or sodermalm_base is None:
                continue
            tcentralen = max(8, tcentralen_base + idx // 2)
            commute = {
                "tcentralen": tcentralen,
                "kista": max(8, kista_base + idx // 2),
                "karolinska": max(8, karolinska_base + idx // 2),
                "sodermalm": max(8, sodermalm_base + idx // 2),
            }

            monthly_rent = int(round(area.metrics.monthly_rent_2br_sek * t["rent_factor"] / 100) * 100)
            estimated_sale = int(round(area.metrics.avg_price_sek_per_sqm * t["sqm"] * t["price_factor"] / 1000) * 1000)
            price_per_sqm = int(round(estimated_sale / t["sqm"]))

            features: List[str] = []
            if area.metrics.green_access_score is not None and area.metrics.green_access_score >= 7.5:
                features.append("green-access")
            if area.metrics.sl_departures_per_hour_peak is not None and area.metrics.sl_departures_per_hour_peak >= 14:
                features.append("high-transit")
            if area.metrics.schools_score is not None and area.metrics.schools_score >= 7.5:
                features.append("schools-strong")
            if area.metrics.pm25_ugm3 is not None and area.metrics.pm25_ugm3 <= 6.5:
                features.append("cleaner-air")
            if area.metrics.broadband_coverage_pct is not None and area.metrics.broadband_coverage_pct >= 94:
                features.append("fast-broadband")

            rows.append(
                ApartmentListing(
                    id=f"{area.id}-{idx + 1}",
                    area_id=area.id,
                    area_name=area.name,
                    municipality=area.municipality,
                    rooms=t["rooms"],
                    sqm=t["sqm"],
                    monthly_rent_sek=monthly_rent,
                    estimated_sale_price_sek=estimated_sale,
                    price_per_sqm_sek=price_per_sqm,
                    commute_times_min=commute,
                    features=features,
                    fit_score=0.0,
                )
            )
    return rows


APARTMENT_LISTINGS: List[ApartmentListing] = build_apartment_listings()


async def fetch_open_meteo(lat: float, lon: float, client: httpx.AsyncClient) -> tuple[float | None, float | None]:
    pm25: float | None = None
    precip_max: float | None = None

    try:
        aq_res = await client.get(
            "https://air-quality-api.open-meteo.com/v1/air-quality",
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": "pm2_5",
                "timezone": "auto",
                "forecast_days": 1,
            },
        )
        aq_res.raise_for_status()
        aq = aq_res.json()
        values = aq.get("hourly", {}).get("pm2_5", [])
        valid = [v for v in values if isinstance(v, (int, float))]
        if valid:
            pm25 = float(valid[-1])
    except Exception:
        pm25 = None

    try:
        wx_res = await client.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "precipitation_sum",
                "timezone": "auto",
                "forecast_days": 7,
            },
        )
        wx_res.raise_for_status()
        wx = wx_res.json()
        rain = wx.get("daily", {}).get("precipitation_sum", [])
        valid = [v for v in rain if isinstance(v, (int, float))]
        if valid:
            precip_max = float(max(valid))
    except Exception:
        precip_max = None

    return pm25, precip_max


async def fetch_overpass_counts(lat: float, lon: float, client: httpx.AsyncClient) -> dict[str, int] | None:
    query = f"""
[out:json][timeout:25];
(
  nwr["leisure"="park"](around:1500,{lat},{lon});
  nwr["amenity"~"school|college|university|kindergarten"](around:1500,{lat},{lon});
  nwr["amenity"~"hospital|clinic|doctors"](around:2500,{lat},{lon});
  way["highway"="cycleway"](around:2000,{lat},{lon});
  relation["route"="bicycle"](around:2000,{lat},{lon});
  way["highway"~"motorway|trunk|primary"](around:1200,{lat},{lon});
);
out tags;
""".strip()

    try:
        res = await client.post("https://overpass-api.de/api/interpreter", data=query)
        res.raise_for_status()
        elements = res.json().get("elements", [])
    except Exception:
        return None

    counts = {
        "parks": 0,
        "schools": 0,
        "healthcare": 0,
        "cycleways": 0,
        "major_roads": 0,
    }

    for element in elements:
        tags = element.get("tags", {})
        leisure = tags.get("leisure")
        amenity = tags.get("amenity")
        highway = tags.get("highway")
        route = tags.get("route")

        if leisure == "park":
            counts["parks"] += 1
        if amenity in {"school", "college", "university", "kindergarten"}:
            counts["schools"] += 1
        if amenity in {"hospital", "clinic", "doctors"}:
            counts["healthcare"] += 1
        if highway == "cycleway" or route == "bicycle":
            counts["cycleways"] += 1
        if highway in {"motorway", "trunk", "primary"}:
            counts["major_roads"] += 1

    return counts


async def enrich_area(area: Area, force_refresh: bool, client: httpx.AsyncClient) -> Area:
    now = time.time()
    cached = ENRICHMENT_CACHE.get(area.id)
    if cached and not force_refresh and now - cached[0] < ENRICHMENT_TTL_SEC:
        return area.model_copy(update={"metrics": cached[1]})

    metrics = area.metrics.model_copy(deep=True)

    pm25, _ = await fetch_open_meteo(area.coordinates.lat, area.coordinates.lon, client)
    if pm25 is not None:
        metrics.pm25_ugm3 = round(pm25, 2)

    ENRICHMENT_CACHE[area.id] = (now, metrics)
    return area.model_copy(update={"metrics": metrics})


async def maybe_enrich_areas(rows: List[Area], live: bool, force_refresh: bool) -> List[Area]:
    if not live:
        return rows

    timeout = httpx.Timeout(12.0, connect=4.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        tasks = [enrich_area(area, force_refresh=force_refresh, client=client) for area in rows]
        return await asyncio.gather(*tasks)


def score_area(
    area: Area,
    budget_sek_per_sqm: int,
    max_commute_min: int,
    destination_coords: Coordinates | None = None,
    priority_price: int = 34,
    priority_commute: int = 33,
    priority_crime: int = 33,
) -> AreaResult:
    m = area.metrics

    effective_commute = estimate_commute_minutes(area, destination_coords) if destination_coords else m.sl_commute_to_tcentralen_min

    if m.avg_price_sek_per_sqm is None:
        price_score = 0.5
    else:
        affordability = normalize(m.avg_price_sek_per_sqm, 25000, 130000, reverse=True)
        budget_fit = 1.0 if m.avg_price_sek_per_sqm <= budget_sek_per_sqm else max(0.0, 1 - (m.avg_price_sek_per_sqm - budget_sek_per_sqm) / 40000)
        price_score = affordability * 0.6 + budget_fit * 0.4

    if effective_commute is None:
        commute_score = 0.5
    else:
        commute_time = normalize(effective_commute, 8, 75, reverse=True)
        commute_limit_fit = 1.0 if effective_commute <= max_commute_min else max(0.0, 1 - (effective_commute - max_commute_min) / 30)
        commute_score = commute_time * 0.7 + commute_limit_fit * 0.3

    if m.crime_rate_per_1000 is None:
        crime_score = 0.5
    else:
        crime_score = normalize(m.crime_rate_per_1000, 20, 90, reverse=True)

    socioeconomics = (
        normalize(m.median_income_sek, 200000, 900000) * 0.5
        + normalize(m.unemployment_rate_pct, 1, 16, reverse=True) * 0.5
    )

    total_priority = max(1, priority_price + priority_commute + priority_crime)
    w_price = priority_price / total_priority
    w_commute = priority_commute / total_priority
    w_crime = priority_crime / total_priority

    priority_score = price_score * w_price + commute_score * w_commute + crime_score * w_crime

    breakdown = {
        "price": round(price_score * 100, 1),
        "commute": round(commute_score * 100, 1),
        "crime": round(crime_score * 100, 1),
        "affordability": round(price_score * 100, 1),
        "mobility": round(commute_score * 100, 1),
        "safety": round(crime_score * 100, 1),
        "socioeconomics": round(socioeconomics * 100, 1),
        "priority_score": round(priority_score * 100, 1),
        "commute_minutes": float(effective_commute) if effective_commute is not None else None,
        "price_data_available": 1.0 if m.avg_price_sek_per_sqm is not None else 0.0,
        "commute_data_available": 1.0 if effective_commute is not None else 0.0,
        "crime_data_available": 1.0 if m.crime_rate_per_1000 is not None else 0.0,
    }

    value_score = round((priority_score * 0.8 + socioeconomics * 0.2) * 100, 1)
    return AreaResult(area=area, value_score=value_score, breakdown=breakdown)


def build_connections(rows: List[AreaResult]) -> List[ConnectionEdge]:
    edges: List[ConnectionEdge] = []

    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            a = rows[i]
            b = rows[j]

            distance_km = haversine_km(a.area.coordinates, b.area.coordinates)
            a_commute = a.area.metrics.sl_commute_to_tcentralen_min
            b_commute = b.area.metrics.sl_commute_to_tcentralen_min
            a_price = a.area.metrics.avg_price_sek_per_sqm
            b_price = b.area.metrics.avg_price_sek_per_sqm
            commute_gap = abs(a_commute - b_commute) if a_commute is not None and b_commute is not None else 22.0
            price_gap = abs(a_price - b_price) if a_price is not None and b_price is not None else 35000.0

            proximity = normalize(distance_km, 0.2, 35, reverse=True)
            commute_similarity = normalize(commute_gap, 0, 45, reverse=True)
            a_dep = a.area.metrics.sl_departures_per_hour_peak
            b_dep = b.area.metrics.sl_departures_per_hour_peak
            transit_strength = normalize(((a_dep or 8) + (b_dep or 8)) / 2.0, 4, 20)
            price_compatibility = normalize(price_gap, 0, 70000, reverse=True)

            score = (
                proximity * 0.35
                + commute_similarity * 0.25
                + transit_strength * 0.2
                + price_compatibility * 0.2
            ) * 100

            if score < 30:
                continue

            est_trip = int(
                round(
                    (((a_commute or 25) + (b_commute or 25)) / 2.0)
                    + distance_km * 1.2
                )
            )
            edges.append(
                ConnectionEdge(
                    from_area_id=a.area.id,
                    to_area_id=b.area.id,
                    connection_score=round(score, 1),
                    estimated_trip_min=est_trip,
                    distance_km=round(distance_km, 2),
                )
            )

    edges.sort(key=lambda x: x.connection_score, reverse=True)
    return edges


def metric_percentile(values: List[float], current: float, higher_is_better: bool) -> float:
    if not values:
        return 0.0
    if higher_is_better:
        below_or_equal = sum(1 for v in values if v <= current)
    else:
        below_or_equal = sum(1 for v in values if v >= current)
    return round((below_or_equal / len(values)) * 100, 1)


def build_drilldown(area_id: str, rows: List[AreaResult]) -> DrilldownResponse:
    target = next((x for x in rows if x.area.id == area_id), None)
    if target is None:
        raise HTTPException(status_code=404, detail="Area not found")

    price_vals = [x.area.metrics.avg_price_sek_per_sqm for x in rows if x.area.metrics.avg_price_sek_per_sqm is not None]
    commute_vals = [x.area.metrics.sl_commute_to_tcentralen_min for x in rows if x.area.metrics.sl_commute_to_tcentralen_min is not None]
    score_vals = [x.value_score for x in rows]
    pm_vals = [x.area.metrics.pm25_ugm3 for x in rows if x.area.metrics.pm25_ugm3 is not None]
    school_vals = [x.area.metrics.schools_score for x in rows if x.area.metrics.schools_score is not None]
    broadband_vals = [x.area.metrics.broadband_coverage_pct for x in rows if x.area.metrics.broadband_coverage_pct is not None]
    crime_vals = [x.area.metrics.crime_rate_per_1000 for x in rows if x.area.metrics.crime_rate_per_1000 is not None]

    percentiles = {
        "value_score": metric_percentile(score_vals, target.value_score, higher_is_better=True),
        "price_per_sqm": metric_percentile(price_vals, target.area.metrics.avg_price_sek_per_sqm, higher_is_better=False) if target.area.metrics.avg_price_sek_per_sqm is not None else 0.0,
        "commute_time": metric_percentile(commute_vals, target.area.metrics.sl_commute_to_tcentralen_min, higher_is_better=False) if target.area.metrics.sl_commute_to_tcentralen_min is not None else 0.0,
        "crime_safety": metric_percentile(crime_vals, target.area.metrics.crime_rate_per_1000, higher_is_better=False) if target.area.metrics.crime_rate_per_1000 is not None else 0.0,
        "schools": metric_percentile(school_vals, target.area.metrics.schools_score, higher_is_better=True) if target.area.metrics.schools_score is not None else 0.0,
        "air_quality": metric_percentile(pm_vals, target.area.metrics.pm25_ugm3, higher_is_better=False) if target.area.metrics.pm25_ugm3 is not None else 0.0,
        "broadband": metric_percentile(broadband_vals, target.area.metrics.broadband_coverage_pct, higher_is_better=True) if target.area.metrics.broadband_coverage_pct is not None else 0.0,
    }

    peer_rows = [x for x in rows if x.area.id != area_id]
    peers_scored: List[tuple[float, AreaResult]] = []
    for peer in peer_rows:
        if peer.area.metrics.avg_price_sek_per_sqm is not None and target.area.metrics.avg_price_sek_per_sqm is not None:
            price_dist = normalize(abs(peer.area.metrics.avg_price_sek_per_sqm - target.area.metrics.avg_price_sek_per_sqm), 0, 70000, reverse=True)
        else:
            price_dist = 0.5
        if peer.area.metrics.sl_commute_to_tcentralen_min is not None and target.area.metrics.sl_commute_to_tcentralen_min is not None:
            commute_dist = normalize(abs(peer.area.metrics.sl_commute_to_tcentralen_min - target.area.metrics.sl_commute_to_tcentralen_min), 0, 45, reverse=True)
        else:
            commute_dist = 0.5
        score_dist = normalize(abs(peer.value_score - target.value_score), 0, 50, reverse=True)
        similarity = round((price_dist * 0.4 + commute_dist * 0.35 + score_dist * 0.25) * 100, 1)
        peers_scored.append((similarity, peer))

    peers_scored.sort(key=lambda x: x[0], reverse=True)
    peers = [
        PeerComparison(
            area_id=peer.area.id,
            name=peer.area.name,
            municipality=peer.area.municipality,
            similarity=similarity,
            value_score=peer.value_score,
            price_diff_sek_per_sqm=(peer.area.metrics.avg_price_sek_per_sqm or 0) - (target.area.metrics.avg_price_sek_per_sqm or 0),
            commute_diff_min=(peer.area.metrics.sl_commute_to_tcentralen_min or 0) - (target.area.metrics.sl_commute_to_tcentralen_min or 0),
        )
        for similarity, peer in peers_scored[:3]
    ]

    opportunities: List[str] = []
    if target.area.metrics.sl_commute_to_tcentralen_min is not None and target.area.metrics.sl_commute_to_tcentralen_min > 25:
        opportunities.append("Commute is relatively long. Consider filtering for <= 25 min and compare peer alternatives.")
    if target.area.metrics.avg_price_sek_per_sqm is not None and target.area.metrics.avg_price_sek_per_sqm > 75000:
        opportunities.append("Price pressure is high here. Compare similar-value peers with lower SEK/sqm.")
    if target.area.metrics.pm25_ugm3 is not None and target.area.metrics.pm25_ugm3 > 6.0:
        opportunities.append("Air quality is weaker than peers. Prioritize lower PM2.5 areas for resilience.")
    if target.area.metrics.schools_score is not None and target.area.metrics.schools_score < 7.0:
        opportunities.append("School access is below top peers. Review areas with higher school percentile.")
    if not opportunities:
        opportunities.append("This area is balanced on key metrics. Focus next on personal constraints and neighborhood fit.")

    return DrilldownResponse(area=target, metric_percentiles=percentiles, peers=peers, opportunities=opportunities)


def expand_area_detail(rows: List[Area], detail_level: Literal["base", "fine", "ultra"]) -> List[Area]:
    # Synthetic micro-zones are intentionally disabled.
    # Keep parameter for backward compatibility, but always return source rows.
    return rows


async def get_scored_rows(
    budget_sek_per_sqm: int,
    max_commute_min: int,
    municipality: str | None,
    live: bool,
    refresh: bool,
    destination_coords: Coordinates | None = None,
    detail_level: Literal["base", "fine", "ultra"] = "base",
    priority_price: int = 34,
    priority_commute: int = 33,
    priority_crime: int = 33,
) -> List[AreaResult]:
    rows = BASE_AREAS
    if municipality:
        rows = [a for a in rows if a.municipality.lower() == municipality.lower()]

    rows = await maybe_enrich_areas(rows, live=live, force_refresh=refresh)
    rows = expand_area_detail(rows, detail_level=detail_level)
    scored = [
        score_area(
            a,
            budget_sek_per_sqm=budget_sek_per_sqm,
            max_commute_min=max_commute_min,
            destination_coords=destination_coords,
            priority_price=priority_price,
            priority_commute=priority_commute,
            priority_crime=priority_crime,
        )
        for a in rows
    ]
    scored.sort(key=lambda x: x.value_score, reverse=True)
    return scored


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok", "areas_source": BASE_AREAS_SOURCE, "areas_count": str(len(BASE_AREAS))}


@app.get("/api/sources", response_model=List[SourceInfo])
def list_sources() -> List[SourceInfo]:
    return SOURCES


@app.get("/api/data_mode")
def data_mode() -> Dict[str, str]:
    return {"areas_source": BASE_AREAS_SOURCE, "areas_count": str(len(BASE_AREAS))}


@app.get("/api/municipalities", response_model=List[str])
def list_municipalities() -> List[str]:
    return sorted({area.municipality for area in BASE_AREAS})


@app.get("/api/geocode", response_model=List[GeocodeCandidate])
async def geocode(q: str = Query(min_length=2, max_length=300), limit: int = Query(default=6, ge=1, le=10)) -> List[GeocodeCandidate]:
    return await geocode_destination(q, limit=limit)


@app.post("/api/refresh")
def refresh_cache() -> Dict[str, str]:
    ENRICHMENT_CACHE.clear()
    GEOCODE_CACHE.clear()
    return {"status": "cache-cleared"}


@app.get("/api/areas", response_model=AreaCollection)
async def list_areas(
    budget_sek_per_sqm: int = Query(70000, ge=1, le=1000000),
    max_commute_min: int = Query(35, ge=10, le=120),
    municipality: str | None = Query(default=None),
    destination: str = Query(default="tcentralen"),
    destination_query: str | None = Query(default=None, max_length=300),
    destination_lat: float | None = Query(default=None, ge=-90, le=90),
    destination_lon: float | None = Query(default=None, ge=-180, le=180),
    detail_level: Literal["base", "fine", "ultra"] = Query(default="base"),
    priority_price: int = Query(34, ge=0, le=100),
    priority_commute: int = Query(33, ge=0, le=100),
    priority_crime: int = Query(33, ge=0, le=100),
    live: bool = Query(default=False, description="Enable live enrichment from free APIs"),
    refresh: bool = Query(default=False, description="Force refresh of live enrichment cache"),
) -> AreaCollection:
    destination_coords, _ = await resolve_destination(
        destination=destination,
        destination_query=destination_query,
        destination_lat=destination_lat,
        destination_lon=destination_lon,
    )
    scored = await get_scored_rows(
        budget_sek_per_sqm=budget_sek_per_sqm,
        max_commute_min=max_commute_min,
        municipality=municipality,
        live=live,
        refresh=refresh,
        destination_coords=destination_coords,
        detail_level=detail_level,
        priority_price=priority_price,
        priority_commute=priority_commute,
        priority_crime=priority_crime,
    )
    return AreaCollection(total=len(scored), items=scored)


@app.get("/api/network", response_model=ConnectionGraph)
async def network(
    budget_sek_per_sqm: int = Query(70000, ge=1, le=1000000),
    max_commute_min: int = Query(35, ge=10, le=120),
    municipality: str | None = Query(default=None),
    destination: str = Query(default="tcentralen"),
    destination_query: str | None = Query(default=None, max_length=300),
    destination_lat: float | None = Query(default=None, ge=-90, le=90),
    destination_lon: float | None = Query(default=None, ge=-180, le=180),
    detail_level: Literal["base", "fine", "ultra"] = Query(default="base"),
    priority_price: int = Query(34, ge=0, le=100),
    priority_commute: int = Query(33, ge=0, le=100),
    priority_crime: int = Query(33, ge=0, le=100),
    live: bool = Query(default=False),
    refresh: bool = Query(default=False),
) -> ConnectionGraph:
    destination_coords, _ = await resolve_destination(
        destination=destination,
        destination_query=destination_query,
        destination_lat=destination_lat,
        destination_lon=destination_lon,
    )
    scored = await get_scored_rows(
        budget_sek_per_sqm=budget_sek_per_sqm,
        max_commute_min=max_commute_min,
        municipality=municipality,
        live=live,
        refresh=refresh,
        destination_coords=destination_coords,
        detail_level=detail_level,
        priority_price=priority_price,
        priority_commute=priority_commute,
        priority_crime=priority_crime,
    )
    edges = build_connections(scored)
    return ConnectionGraph(total_edges=len(edges), edges=edges)


@app.get("/api/drilldown/{area_id}", response_model=DrilldownResponse)
async def drilldown(
    area_id: str,
    budget_sek_per_sqm: int = Query(70000, ge=1, le=1000000),
    max_commute_min: int = Query(35, ge=10, le=120),
    municipality: str | None = Query(default=None),
    destination: str = Query(default="tcentralen"),
    destination_query: str | None = Query(default=None, max_length=300),
    destination_lat: float | None = Query(default=None, ge=-90, le=90),
    destination_lon: float | None = Query(default=None, ge=-180, le=180),
    detail_level: Literal["base", "fine", "ultra"] = Query(default="base"),
    priority_price: int = Query(34, ge=0, le=100),
    priority_commute: int = Query(33, ge=0, le=100),
    priority_crime: int = Query(33, ge=0, le=100),
    live: bool = Query(default=False),
    refresh: bool = Query(default=False),
) -> DrilldownResponse:
    destination_coords, _ = await resolve_destination(
        destination=destination,
        destination_query=destination_query,
        destination_lat=destination_lat,
        destination_lon=destination_lon,
    )
    scored = await get_scored_rows(
        budget_sek_per_sqm=budget_sek_per_sqm,
        max_commute_min=max_commute_min,
        municipality=municipality,
        live=live,
        refresh=refresh,
        destination_coords=destination_coords,
        detail_level=detail_level,
        priority_price=priority_price,
        priority_commute=priority_commute,
        priority_crime=priority_crime,
    )
    return build_drilldown(area_id=area_id, rows=scored)


@app.get("/api/listings", response_model=ApartmentCollection)
async def listings(
    destination: str = Query(default="tcentralen"),
    destination_query: str | None = Query(default=None, max_length=300),
    destination_lat: float | None = Query(default=None, ge=-90, le=90),
    destination_lon: float | None = Query(default=None, ge=-180, le=180),
    max_commute_min: int = Query(default=40, ge=5, le=180),
    min_rooms: float = Query(default=1.0, ge=0.5, le=8.0),
    min_sqm: int = Query(default=20, ge=15, le=250),
    max_monthly_rent_sek: int | None = Query(default=None, ge=3000, le=100000),
    max_total_price_sek: int | None = Query(default=None, ge=100000, le=30000000),
    max_price_per_sqm_sek: int | None = Query(default=None, ge=10000, le=250000),
    municipality: str | None = Query(default=None),
    area_id: str | None = Query(default=None),
    sort_by: Literal["fit", "rent", "commute", "price_per_sqm"] = Query(default="fit"),
    limit: int = Query(default=100, ge=1, le=500),
) -> ApartmentCollection:
    destination_coords, destination_label = await resolve_destination(
        destination=destination,
        destination_query=destination_query,
        destination_lat=destination_lat,
        destination_lon=destination_lon,
    )

    rows = APARTMENT_LISTINGS

    if municipality:
        rows = [x for x in rows if x.municipality.lower() == municipality.lower()]
    if area_id:
        rows = [x for x in rows if x.area_id == area_id]

    filtered: List[ApartmentListing] = []
    for row in rows:
        area = next((a for a in BASE_AREAS if a.id == row.area_id), None)
        if area is None:
            continue
        commute = estimate_commute_minutes(area, destination_coords)
        if commute is None:
            continue
        if commute > max_commute_min:
            continue
        if row.rooms < min_rooms:
            continue
        if row.sqm < min_sqm:
            continue
        if max_monthly_rent_sek is not None and row.monthly_rent_sek > max_monthly_rent_sek:
            continue
        if max_total_price_sek is not None and row.estimated_sale_price_sek > max_total_price_sek:
            continue
        if max_price_per_sqm_sek is not None and row.price_per_sqm_sek > max_price_per_sqm_sek:
            continue

        commute_fit = normalize(commute, 8, 90, reverse=True)
        price_fit = normalize(row.price_per_sqm_sek, 30000, 110000, reverse=True)
        size_fit = normalize(row.sqm, 20, 120)
        room_fit = normalize(row.rooms, 1, 5)
        affordability_fit = normalize(row.monthly_rent_sek, 8000, 32000, reverse=True)
        fit = round((commute_fit * 0.35 + price_fit * 0.25 + affordability_fit * 0.2 + size_fit * 0.1 + room_fit * 0.1) * 100, 1)

        filtered.append(
            row.model_copy(
                update={
                    "fit_score": fit,
                    "destination_label": destination_label,
                    "commute_to_destination_min": commute,
                }
            )
        )

    if sort_by == "rent":
        filtered.sort(key=lambda x: x.monthly_rent_sek)
    elif sort_by == "commute":
        filtered.sort(key=lambda x: x.commute_to_destination_min or 9999)
    elif sort_by == "price_per_sqm":
        filtered.sort(key=lambda x: x.price_per_sqm_sek)
    else:
        filtered.sort(key=lambda x: x.fit_score, reverse=True)

    out = filtered[:limit]
    return ApartmentCollection(total=len(out), items=out)


@app.get("/api/areas/{area_id}", response_model=AreaResult)
async def get_area(
    area_id: str,
    budget_sek_per_sqm: int = 70000,
    max_commute_min: int = 35,
    priority_price: int = 34,
    priority_commute: int = 33,
    priority_crime: int = 33,
    live: bool = False,
    refresh: bool = False,
) -> AreaResult:
    area = next((a for a in BASE_AREAS if a.id == area_id), None)
    if area is None:
        raise HTTPException(status_code=404, detail="Area not found")

    if live:
        timeout = httpx.Timeout(12.0, connect=4.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            area = await enrich_area(area, force_refresh=refresh, client=client)

    return score_area(
        area,
        budget_sek_per_sqm=budget_sek_per_sqm,
        max_commute_min=max_commute_min,
        priority_price=priority_price,
        priority_commute=priority_commute,
        priority_crime=priority_crime,
    )
