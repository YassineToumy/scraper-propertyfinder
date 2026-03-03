#!/usr/bin/env python3
"""
PropertyFinder — MongoDB (locations) -> PostgreSQL Sync + Archive Checker
Incremental: only syncs new docs, archives dead listings.

Usage:
    python sync.py          # Run once (sync + archive)
    python sync.py --loop   # Loop every CYCLE_SLEEP seconds
    python sync.py --sync-only
    python sync.py --archive-only
"""

import os
import re
import time
import logging
import argparse
import requests
from datetime import datetime, timezone

from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

MONGO_URI = os.environ["MONGODB_URI"]
MONGO_DB = os.getenv("MONGO_PROPERTYFINDER_DB", "propertyfinder")
MONGO_COL = os.getenv("MONGO_PROPERTYFINDER_COL", "locations")

PG_DSN = os.environ["POSTGRES_DSN"]
PG_TABLE = os.getenv("PG_TABLE_PROPERTYFINDER", "propertyfinder_listings")
PG_ARCHIVE = os.getenv("PG_ARCHIVE_PROPERTYFINDER", "propertyfinder_archive")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
CYCLE_SLEEP = int(os.getenv("CYCLE_SLEEP", "86400"))
ARCHIVE_DELAY = 2
ARCHIVE_TIMEOUT = 15

HOMEPAGE_REDIRECTS = {
    "https://www.propertyfinder.eg",
    "https://www.propertyfinder.eg/en",
}

CHECK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("propertyfinder-sync")

# ============================================================
# HELPERS
# ============================================================

def to_pg_array(lst):
    if not lst or not isinstance(lst, list):
        return None
    cleaned = [str(x) for x in lst if x]
    return cleaned if cleaned else None


def to_pg_timestamp(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, dict) and "$date" in v:
        try:
            return datetime.fromisoformat(v["$date"].replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    if isinstance(v, (int, float)):
        try:
            if v > 1e12:
                v = v / 1000
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except (OSError, ValueError, OverflowError):
            return None
    return None


def parse_int_safe(v):
    if v is None:
        return None
    if isinstance(v, int):
        return v
    try:
        return int(str(v).strip())
    except (ValueError, TypeError):
        return None


def parse_price_string(s):
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    cleaned = re.sub(r"[^\d.]", "", str(s).replace(",", "").replace("\xa0", ""))
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


PF_KNOWN_AMENITIES = {
    "Furnished", "Built in Wardrobes", "Central A/C", "Covered Parking",
    "Kitchen Appliances", "Private Garden", "Study", "Shared Spa", "Security",
    "Swimming Pool", "Gym", "Elevator", "Maid's Room", "Maids Room",
    "Storage Room", "Pets Allowed", "Concierge", "Children's Play Area",
    "BBQ Area", "Jacuzzi", "Sauna", "Steam Room", "View of Landmark",
    "View of Water", "Shared Pool", "Private Pool", "Internet", "Balcony",
    "Walk-in Closet", "Built in Kitchen Appliances",
}


def clean_pf_amenities(raw):
    if not raw or not isinstance(raw, list):
        return None
    cleaned = [a for a in raw if a in PF_KNOWN_AMENITIES]
    return cleaned if cleaned else None


def clean_pf_description(raw):
    if not raw:
        return None
    if "BuyRentNew projects" in raw or "Log inApartments" in raw:
        m = re.search(r"sqm(.+?)(?:See full description|Property details)", raw, re.DOTALL)
        if m and len(m.group(1).strip()) > 20:
            return m.group(1).strip()
        m = re.search(
            r"((?:Apartment|Villa|Flat|Furnished|Brand new|Luxury|Spacious).*?)"
            r"(?:See full description|Property details|Property Type)",
            raw, re.DOTALL | re.IGNORECASE,
        )
        if m:
            return m.group(1).strip()
        return None
    return raw


# ============================================================
# ROW BUILDER
# ============================================================

def build_row(d):
    amenities = clean_pf_amenities(d.get("amenities"))
    description = clean_pf_description(d.get("description"))
    ps = d.get("property_size") or {}
    pi = d.get("price_insights") or {}
    furn = d.get("furnished")
    is_furnished = True if furn == "furnished" else (False if furn else None)
    images = to_pg_array(d.get("images"))
    return (
        d.get("property_id"), d.get("reference"), d.get("url"),
        d.get("city"), d.get("district"), d.get("compound"),
        d.get("location_full"), d.get("property_type"),
        parse_int_safe(ps.get("sqm")), parse_int_safe(ps.get("sqft")),
        parse_int_safe(d.get("bedrooms")), parse_int_safe(d.get("bathrooms")),
        is_furnished, d.get("available_from"),
        d.get("title"), parse_price_string(d.get("price_value")),
        d.get("price_raw"), d.get("currency", "EGP"),
        d.get("price_period", "monthly"),
        description, amenities, images,
        len(images) if images else 0,
        parse_price_string(pi.get("avg_rent")),
        parse_int_safe(pi.get("avg_size")),
        parse_int_safe(pi.get("vs_avg_pct")), pi.get("vs_avg_dir"),
        d.get("agent_name"), d.get("agency_name"),
        d.get("listed_date"), to_pg_timestamp(d.get("scraped_at")),
        to_pg_timestamp(d.get("first_seen")),
    )


INSERT_SQL = """
    INSERT INTO propertyfinder_listings (
        property_id, reference, url, city, district,
        compound, location_full, property_type, surface_sqm, surface_sqft,
        bedrooms, bathrooms, is_furnished, available_from, title,
        price_value, price_raw, currency, price_period,
        description, amenities, images, images_count,
        avg_rent_area, avg_size_area, vs_avg_pct, vs_avg_dir,
        agent_name, agency_name, listed_date, scraped_at, first_seen
    ) VALUES %s
    ON CONFLICT (property_id) DO NOTHING
"""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS propertyfinder_listings (
    id SERIAL PRIMARY KEY,
    property_id VARCHAR(50) UNIQUE NOT NULL,
    reference VARCHAR(100),
    url TEXT,
    city VARCHAR(200),
    district VARCHAR(200),
    compound VARCHAR(300),
    location_full VARCHAR(500),
    property_type VARCHAR(100),
    surface_sqm INTEGER,
    surface_sqft INTEGER,
    bedrooms INTEGER,
    bathrooms INTEGER,
    is_furnished BOOLEAN,
    available_from VARCHAR(50),
    title VARCHAR(500),
    price_value DOUBLE PRECISION,
    price_raw VARCHAR(100),
    currency VARCHAR(5) DEFAULT 'EGP',
    price_period VARCHAR(20) DEFAULT 'monthly',
    description TEXT,
    amenities TEXT[],
    images TEXT[],
    images_count INTEGER DEFAULT 0,
    avg_rent_area DOUBLE PRECISION,
    avg_size_area INTEGER,
    vs_avg_pct INTEGER,
    vs_avg_dir VARCHAR(10),
    agent_name VARCHAR(300),
    agency_name VARCHAR(300),
    listed_date VARCHAR(50),
    scraped_at TIMESTAMPTZ,
    first_seen TIMESTAMPTZ,
    synced_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pf_city ON propertyfinder_listings(city);
CREATE INDEX IF NOT EXISTS idx_pf_price ON propertyfinder_listings(price_value);
"""

# ============================================================
# SCHEMA + ARCHIVE
# ============================================================

def ensure_schema(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(SCHEMA_SQL)
    pg_conn.commit()
    log.info("✅ Table propertyfinder_listings ensured")


def ensure_archive_table(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
        (PG_ARCHIVE,)
    )
    if cur.fetchone()[0]:
        return
    cur.execute(f"""
        CREATE TABLE {PG_ARCHIVE} (
            LIKE {PG_TABLE} INCLUDING DEFAULTS INCLUDING GENERATED
        )
    """)
    cur.execute(f"""
        ALTER TABLE {PG_ARCHIVE}
            ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ DEFAULT NOW(),
            ADD COLUMN IF NOT EXISTS archive_reason VARCHAR(50) DEFAULT 'listing_removed'
    """)
    cur.execute(f"""
        SELECT conname FROM pg_constraint
        WHERE conrelid = '{PG_ARCHIVE}'::regclass
        AND contype IN ('u', 'p')
        AND conname != '{PG_ARCHIVE}_pkey'
    """)
    for row in cur.fetchall():
        cur.execute(f"ALTER TABLE {PG_ARCHIVE} DROP CONSTRAINT IF EXISTS {row[0]}")
    pg_conn.commit()
    log.info(f"✅ Archive table {PG_ARCHIVE} created")


# ============================================================
# SYNC
# ============================================================

def get_existing_ids(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(f"SELECT property_id FROM {PG_TABLE} WHERE property_id IS NOT NULL")
    return {str(row[0]) for row in cur.fetchall()}


def _flush_batch(pg_conn, batch, stats):
    try:
        cur = pg_conn.cursor()
        execute_values(cur, INSERT_SQL, batch)
        pg_conn.commit()
        stats["new_synced"] += len(batch)
    except Exception as e:
        pg_conn.rollback()
        log.error(f"Batch insert error: {e}")
        recovered = 0
        for row in batch:
            try:
                cur = pg_conn.cursor()
                execute_values(cur, INSERT_SQL, [row])
                pg_conn.commit()
                recovered += 1
            except Exception:
                pg_conn.rollback()
                stats["errors"] += 1
        stats["new_synced"] += recovered


def sync(mongo_col, pg_conn):
    stats = {"total_mongo": 0, "already_in_pg": 0, "new_synced": 0, "errors": 0}
    stats["total_mongo"] = mongo_col.count_documents({})

    existing_ids = get_existing_ids(pg_conn)
    stats["already_in_pg"] = len(existing_ids)

    log.info(f"  MongoDB: {stats['total_mongo']} | PostgreSQL: {stats['already_in_pg']}")

    if stats["total_mongo"] == 0:
        log.info("  No documents in MongoDB — skipping")
        return stats

    batch = []
    for doc in mongo_col.find({}, batch_size=BATCH_SIZE):
        doc_id = doc.get("property_id")
        if not doc_id or str(doc_id) in existing_ids:
            continue
        try:
            batch.append(build_row(doc))
        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                log.warning(f"  Row build error (property_id={doc_id}): {e}")
        if len(batch) >= BATCH_SIZE:
            _flush_batch(pg_conn, batch, stats)
            batch = []
            log.info(f"  Synced {stats['new_synced']} so far...")

    if batch:
        _flush_batch(pg_conn, batch, stats)

    return stats


# ============================================================
# ARCHIVE
# ============================================================

def check_url_alive(url):
    try:
        resp = requests.head(
            url, headers=CHECK_HEADERS,
            timeout=ARCHIVE_TIMEOUT, allow_redirects=True
        )
        if resp.status_code in (404, 410):
            return False
        if resp.url.rstrip("/") in HOMEPAGE_REDIRECTS:
            return False
        if resp.status_code == 403:
            return True
        if resp.status_code < 400:
            return True
        if resp.status_code >= 500:
            return True
        return False
    except requests.RequestException:
        return True


def archive_listing(pg_conn, unique_val, reason="listing_removed"):
    cur = pg_conn.cursor()
    try:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND column_name NOT IN ('archived_at', 'archive_reason')
            ORDER BY ordinal_position
        """, (PG_TABLE,))
        columns = [r[0] for r in cur.fetchall()]
        cols_str = ", ".join(columns)
        cur.execute(f"""
            INSERT INTO {PG_ARCHIVE} ({cols_str}, archived_at, archive_reason)
            SELECT {cols_str}, NOW(), %s FROM {PG_TABLE} WHERE property_id = %s
        """, (reason, unique_val))
        cur.execute(f"DELETE FROM {PG_TABLE} WHERE property_id = %s", (unique_val,))
        pg_conn.commit()
        return True
    except Exception as e:
        pg_conn.rollback()
        log.error(f"Archive error for {unique_val}: {e}")
        return False


def archive_check(pg_conn):
    stats = {"checked": 0, "alive": 0, "archived": 0, "errors": 0}
    cur = pg_conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(f"SELECT property_id, url FROM {PG_TABLE} WHERE url IS NOT NULL")
    rows = cur.fetchall()
    log.info(f"  {len(rows)} listings to check")

    for i, row in enumerate(rows):
        url = row.get("url")
        if not url:
            continue
        alive = check_url_alive(url)
        stats["checked"] += 1
        if alive:
            stats["alive"] += 1
        else:
            if archive_listing(pg_conn, row["property_id"]):
                stats["archived"] += 1
                log.info(f"  📦 Archived: {row['property_id']}")
            else:
                stats["errors"] += 1
        if (i + 1) % 50 == 0:
            log.info(f"  Progress: {i+1}/{len(rows)} | alive={stats['alive']} archived={stats['archived']}")
        time.sleep(ARCHIVE_DELAY)

    return stats


# ============================================================
# MAIN CYCLE
# ============================================================

def run_cycle(mongo_col, pg_conn, do_sync=True, do_archive=True):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"\n{'='*60}")
    log.info(f"[🇪🇬 PROPERTYFINDER] CYCLE START: {now}")
    log.info(f"{'='*60}")

    ensure_schema(pg_conn)
    ensure_archive_table(pg_conn)

    if do_sync:
        log.info("\n--- PHASE 1: SYNC MongoDB -> PostgreSQL ---")
        stats = sync(mongo_col, pg_conn)
        log.info(f"  ✅ +{stats['new_synced']} new | {stats['errors']} errors")

    if do_archive:
        log.info("\n--- PHASE 2: ARCHIVE CHECK ---")
        stats = archive_check(pg_conn)
        log.info(f"  ✅ {stats['checked']} checked | {stats['alive']} alive | "
                 f"{stats['archived']} archived | {stats['errors']} errors")

    cur = pg_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {PG_TABLE}")
    active = cur.fetchone()[0]
    try:
        cur.execute(f"SELECT COUNT(*) FROM {PG_ARCHIVE}")
        archived = cur.fetchone()[0]
    except Exception:
        pg_conn.rollback()
        archived = 0

    log.info(f"\n📊 {PG_TABLE}: {active} active | {PG_ARCHIVE}: {archived} archived")
    log.info("✅ CYCLE COMPLETE\n")


def main():
    parser = argparse.ArgumentParser(description="PropertyFinder Sync + Archive")
    parser.add_argument("--sync-only", action="store_true")
    parser.add_argument("--archive-only", action="store_true")
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()

    do_sync = not args.archive_only
    do_archive = not args.sync_only

    log.info("Connecting to MongoDB...")
    mongo = MongoClient(MONGO_URI)
    mongo.admin.command("ping")
    log.info("✅ MongoDB connected")

    log.info("Connecting to PostgreSQL...")
    pg = psycopg2.connect(PG_DSN)
    log.info("✅ PostgreSQL connected")

    try:
        if args.loop:
            while True:
                try:
                    col = mongo[MONGO_DB][MONGO_COL]
                    run_cycle(col, pg, do_sync, do_archive)
                except Exception as e:
                    log.error(f"Cycle error: {e}")
                    try:
                        pg.close()
                    except Exception:
                        pass
                    pg = psycopg2.connect(PG_DSN)
                log.info(f"💤 Sleeping {CYCLE_SLEEP}s until next cycle...")
                time.sleep(CYCLE_SLEEP)
        else:
            col = mongo[MONGO_DB][MONGO_COL]
            run_cycle(col, pg, do_sync, do_archive)

    except KeyboardInterrupt:
        log.info("\n⚠️ Stopped by user")
    finally:
        pg.close()
        mongo.close()
        log.info("🔌 Connections closed")


if __name__ == "__main__":
    main()
