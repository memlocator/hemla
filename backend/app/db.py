"""
SQLite database access for Hemla.

DB location: backend/data/hemla.db  (gitignored via backend/data/)

Tables
------
sold_listings — one row per sold bostadsrätt listing.
    id                  TEXT PRIMARY KEY  (Hemnet SaleCard ID)
    listing_id          TEXT
    deso_id             TEXT              (NULL until aggregate step assigns it)
    lat, lon            REAL
    final_price_sek     INTEGER
    sqm                 INTEGER
    price_per_sqm_sek   INTEGER
    sold_at             INTEGER           (unix timestamp)
    rooms               TEXT
    street_address      TEXT
    location_description TEXT
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "hemla.db"

_DDL = """
CREATE TABLE IF NOT EXISTS sold_listings (
    id                   TEXT PRIMARY KEY,
    listing_id           TEXT,
    deso_id              TEXT,
    lat                  REAL NOT NULL,
    lon                  REAL NOT NULL,
    final_price_sek      INTEGER NOT NULL,
    sqm                  INTEGER NOT NULL,
    price_per_sqm_sek    INTEGER NOT NULL,
    sold_at              INTEGER,
    rooms                TEXT,
    street_address       TEXT,
    location_description TEXT
);
CREATE INDEX IF NOT EXISTS idx_sl_deso  ON sold_listings(deso_id);
CREATE INDEX IF NOT EXISTS idx_sl_time  ON sold_listings(sold_at DESC);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(_DDL)


def db_exists() -> bool:
    return DB_PATH.exists()


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def get_sold_listings_for_deso(deso_id: str, limit: int = 50) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, street_address, final_price_sek, price_per_sqm_sek,
                   sqm, rooms, sold_at, lat, lon, location_description
            FROM sold_listings
            WHERE deso_id = ?
            ORDER BY sold_at DESC
            LIMIT ?
            """,
            (deso_id, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def count_sold_listings_for_deso(deso_id: str) -> int:
    with get_conn() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM sold_listings WHERE deso_id = ?", (deso_id,)
        ).fetchone()[0]


def get_all_sold_listings_compact() -> list[dict]:
    """Compact dicts for map-layer rendering (single-letter keys to keep payload small)."""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT street_address, price_per_sqm_sek, final_price_sek,
                   CASE
                     WHEN sqm > 400 AND final_price_sek IS NOT NULL
                          AND ABS(CAST(final_price_sek AS REAL) / (sqm / 10.0) - price_per_sqm_sek) < price_per_sqm_sek * 0.25
                     THEN sqm / 10
                     ELSE sqm
                   END AS sqm,
                   rooms, sold_at, lat, lon, deso_id
            FROM sold_listings
            WHERE lat IS NOT NULL AND lon IS NOT NULL
              AND price_per_sqm_sek > 0
              AND sqm >= 10
            """
        ).fetchall()
    return [
        {
            "a": r["street_address"] or "",
            "p": r["price_per_sqm_sek"],
            "f": r["final_price_sek"],
            "s": r["sqm"],
            "r": r["rooms"] or "",
            "t": r["sold_at"] or 0,
            "la": r["lat"],
            "lo": r["lon"],
            "d": r["deso_id"] or "",
        }
        for r in rows
    ]


def total_listings() -> int:
    if not db_exists():
        return 0
    with get_conn() as conn:
        return conn.execute("SELECT COUNT(*) FROM sold_listings").fetchone()[0]


def total_listings_with_deso() -> int:
    if not db_exists():
        return 0
    with get_conn() as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM sold_listings WHERE deso_id IS NOT NULL"
        ).fetchone()[0]


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def insert_listing(conn: sqlite3.Connection, row: dict) -> None:
    """INSERT OR IGNORE a single listing row. Call within an open connection."""
    conn.execute(
        """
        INSERT OR IGNORE INTO sold_listings
            (id, listing_id, deso_id, lat, lon,
             final_price_sek, sqm, price_per_sqm_sek,
             sold_at, rooms, street_address, location_description)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["id"],
            row.get("listing_id", ""),
            row.get("deso_id"),  # may be None
            float(row["lat"]),
            float(row["lon"]),
            int(row["final_price_sek"]),
            int(row["sqm"]),
            int(row["price_per_sqm_sek"]),
            int(row["sold_at"]) if row.get("sold_at") else None,
            row.get("rooms", ""),
            row.get("street_address", ""),
            row.get("location_description", ""),
        ),
    )


def set_deso_id(conn: sqlite3.Connection, listing_id: str, deso_id: str) -> None:
    conn.execute(
        "UPDATE sold_listings SET deso_id = ? WHERE id = ?",
        (deso_id, listing_id),
    )
