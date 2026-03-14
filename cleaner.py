#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PropertyFinder Data Cleaner — Locations (Incremental)
Normalise raw MongoDB → locations_clean collection.

Usage:
    python cleaner.py              # Incremental (only new docs)
    python cleaner.py --full       # Drop + recreate
    python cleaner.py --dry-run    # Preview, no writes
    python cleaner.py --sample 5   # Show N docs after cleaning
"""

import os
import re
import html
import unicodedata
import argparse
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.errors import BulkWriteError
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

MONGODB_URI = os.getenv("MONGODB_URI", "")
if not MONGODB_URI:
    raise RuntimeError("MONGODB_URI is not set or empty")

MONGODB_DATABASE  = os.getenv("MONGO_PROPERTYFINDER_DB", "propertyfinder")
SOURCE_COLLECTION = os.getenv("MONGO_PROPERTYFINDER_COL", "locations")
CLEAN_COLLECTION  = os.getenv("MONGO_PROPERTYFINDER_COL_CLEAN", "locations_clean")
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "500"))

# EGP monthly rent thresholds
MIN_PRICE   = 1_000
MAX_PRICE   = 5_000_000
MIN_SURFACE = 10
MAX_SURFACE = 2_000
MAX_ROOMS   = 30

# ============================================================
# CLEANING HELPERS
# ============================================================

def parse_price(doc: dict) -> tuple[float | None, str]:
    """Return (price_monthly_egp, currency)."""
    pv = doc.get("price_value")
    period = doc.get("price_period", "monthly")
    if pv is None:
        return None, "EGP"
    try:
        pv = float(pv)
    except (TypeError, ValueError):
        return None, "EGP"
    if period == "yearly":
        pv = round(pv / 12, 2)
    return pv, "EGP"


def parse_surface(doc: dict) -> tuple[float | None, float | None]:
    """Return (surface_m2, surface_sqft) from property_size dict."""
    ps = doc.get("property_size")
    if not isinstance(ps, dict):
        return None, None
    sqm  = ps.get("sqm")
    sqft = ps.get("sqft")
    if sqm is None and sqft is not None:
        sqm = round(sqft * 0.0929, 2)
    if sqft is None and sqm is not None:
        sqft = round(sqm * 10.7639, 2)
    return (float(sqm) if sqm else None, float(sqft) if sqft else None)


def parse_bedrooms(doc: dict) -> int | None:
    v = doc.get("bedrooms")
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def clean_description(raw: str | None) -> str | None:
    """Strip HTML, decode entities, normalize accents."""
    if not raw:
        return None
    text = re.sub(r"<[^>]+>", " ", raw)
    text = html.unescape(text)
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = text.strip()
    return text if len(text) > 20 else None


def clean_document(doc: dict) -> dict:
    c = {}

    source_id = doc.get("property_id") or doc.get("reference")
    if not source_id:
        # Derive from URL
        m = re.search(r"-([A-Za-z0-9]{5,})\.html$", doc.get("url", ""))
        source_id = m.group(1) if m else None
    c["source_id"]       = str(source_id) if source_id else None
    c["source"]          = "propertyfinder"
    c["country"]         = "EG"
    c["transaction_type"] = "rent"
    c["reference"]       = doc.get("reference")

    c["url"] = doc.get("url")

    prop_type = (doc.get("property_type") or "").lower()
    type_map = {
        "apartment": "apartment", "flat": "apartment",
        "villa": "house", "house": "house", "townhouse": "house",
        "duplex": "apartment", "penthouse": "apartment", "studio": "apartment",
    }
    c["property_type"] = type_map.get(prop_type, prop_type or None)

    c["city"]     = doc.get("city")
    c["district_name"] = doc.get("district")

    price, currency = parse_price(doc)
    c["price"]    = price
    c["currency"] = currency

    surface_m2, surface_sqft = parse_surface(doc)
    c["surface_m2"]   = surface_m2
    c["surface_sqft"] = surface_sqft

    c["bedrooms"]  = parse_bedrooms(doc)
    c["bathrooms"] = doc.get("bathrooms")

    c["is_furnished"] = doc.get("furnished") == "furnished"

    c["description"] = clean_description(doc.get("description"))
    c["title"]       = doc.get("title")

    images = doc.get("images")
    if isinstance(images, list):
        c["photos"]       = images
        c["photos_count"] = len(images)
    else:
        c["photos"]       = []
        c["photos_count"] = 0

    c["agency_name"] = doc.get("agency_name")

    amenities = doc.get("amenities")
    if isinstance(amenities, list) and amenities:
        c["features"] = amenities

    if price and surface_m2 and surface_m2 > 0:
        c["price_per_m2"]   = round(price / surface_m2, 2)
    if price and surface_sqft and surface_sqft > 0:
        c["price_per_sqft"] = round(price / surface_sqft, 2)
    if price and c.get("bedrooms") and c["bedrooms"] > 0:
        c["price_per_bedroom"] = round(price / c["bedrooms"], 2)

    c["scraped_at"] = doc.get("scraped_at")
    c["cleaned_at"] = datetime.now(timezone.utc)

    return {k: v for k, v in c.items() if v is not None and v != [] and v != ""}


# ============================================================
# VALIDATION
# ============================================================

def validate(doc: dict) -> tuple[bool, str | None]:
    price = doc.get("price")
    if not price or price < MIN_PRICE or price > MAX_PRICE:
        return False, "invalid_price"

    if not doc.get("source_id"):
        return False, "missing_source_id"

    if not doc.get("city"):
        return False, "missing_city"

    surface = doc.get("surface_m2")
    if surface and (surface < MIN_SURFACE or surface > MAX_SURFACE):
        return False, "invalid_surface"

    rooms = doc.get("bedrooms")
    if rooms and rooms > MAX_ROOMS:
        return False, "aberrant_rooms"

    return True, None


# ============================================================
# DB HELPERS
# ============================================================

def connect_db():
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DATABASE]
    return client, db


def ensure_indexes(col):
    col.create_index([("source_id", ASCENDING)], unique=True, name="source_id_unique")
    col.create_index([("city", ASCENDING)])
    col.create_index([("price", ASCENDING)])
    col.create_index([("surface_m2", ASCENDING)])
    col.create_index([("property_type", ASCENDING)])
    col.create_index([("country", ASCENDING)])
    return col


def insert_batch(col, batch: list) -> tuple[int, int]:
    if not batch:
        return 0, 0
    ops = [
        UpdateOne(
            {"source_id": doc["source_id"]},
            {"$set": doc},
            upsert=True,
        )
        for doc in batch if doc.get("source_id")
    ]
    if not ops:
        return 0, 0
    r = col.bulk_write(ops, ordered=False)
    return r.upserted_count, r.modified_count


# ============================================================
# PIPELINE
# ============================================================

def run(source_col, clean_col, dry_run=False):
    total = source_col.count_documents({})
    print(f"   Source total: {total} docs")

    if not dry_run and clean_col is not None:
        existing_ids = {
            d.get("source_id") for d in clean_col.find({}, {"source_id": 1, "_id": 0})
            if d.get("source_id")
        }
        print(f"   Already cleaned: {len(existing_ids)}")
    else:
        existing_ids = set()

    # Build query using property_id or reference
    if existing_ids:
        query = {"$and": [
            {"$or": [
                {"property_id": {"$nin": list(existing_ids)}},
                {"reference":   {"$nin": list(existing_ids)}},
            ]}
        ]}
    else:
        query = {}

    pending = source_col.count_documents(query)
    print(f"   Pending: {pending}\n")

    if pending == 0:
        print("   Nothing new to clean.")
        return

    stats = {
        "cleaned": 0, "inserted": 0, "updated": 0,
        "invalid_price": 0, "missing_source_id": 0, "missing_city": 0,
        "invalid_surface": 0, "aberrant_rooms": 0, "errors": 0,
    }

    batch = []
    cursor = source_col.find(query, batch_size=BATCH_SIZE, no_cursor_timeout=True)
    try:
        for i, doc in enumerate(cursor):
            try:
                cleaned = clean_document(doc)
                stats["cleaned"] += 1

                valid, reason = validate(cleaned)
                if not valid:
                    stats[reason] = stats.get(reason, 0) + 1
                    continue

                cleaned.pop("_id", None)

                if dry_run:
                    stats["inserted"] += 1
                    continue

                batch.append(cleaned)
                if len(batch) >= BATCH_SIZE:
                    ins, upd = insert_batch(clean_col, batch)
                    stats["inserted"] += ins
                    stats["updated"]  += upd
                    batch = []
                    print(f"   {i+1}/{pending} cleaned …", end="\r", flush=True)

            except Exception as e:
                stats["errors"] += 1
                if stats["errors"] <= 5:
                    print(f"\n   Error on {doc.get('property_id')}: {str(e)[:100]}")

        if batch and not dry_run:
            ins, upd = insert_batch(clean_col, batch)
            stats["inserted"] += ins
            stats["updated"]  += upd
    finally:
        cursor.close()

    print_stats(stats, dry_run)


def print_stats(s, dry_run=False):
    print(f"\n{'='*60}")
    print(f"CLEANING RESULTS {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*60}")
    print(f"   Processed:  {s['cleaned']}")
    print(f"   Inserted:   {s['inserted']}")
    print(f"   Updated:    {s['updated']}")
    rejected = s["cleaned"] - s["inserted"] - s["updated"]
    if rejected > 0:
        print(f"   Rejected:   {rejected}")
        for k in ("invalid_price", "missing_source_id", "missing_city", "invalid_surface", "aberrant_rooms"):
            if s.get(k):
                print(f"      {k}: {s[k]}")
    if s["errors"]:
        print(f"   Errors:     {s['errors']}")
    print(f"{'='*60}")


def show_sample(col, n=3):
    print(f"\nSAMPLE DOCS ({n}):")
    for doc in col.find({}, {"_id": 0}).limit(n):
        print("─" * 60)
        for k, v in doc.items():
            if k == "photos":
                print(f"   {k}: [{len(v)} urls]")
            elif k == "description":
                print(f"   {k}: {str(v)[:80]}...")
            else:
                print(f"   {k}: {v}")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="PropertyFinder Cleaner")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--full",    action="store_true")
    parser.add_argument("--sample",  type=int, default=0)
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("PROPERTYFINDER CLEANER — LOCATIONS")
    print(f"   {SOURCE_COLLECTION} → {CLEAN_COLLECTION}")
    mode = "DRY RUN" if args.dry_run else ("FULL RE-CLEAN" if args.full else "INCREMENTAL")
    print(f"   Mode: {mode}")
    print("=" * 60 + "\n")

    client, db = connect_db()
    source_col = db[SOURCE_COLLECTION]

    if args.dry_run:
        run(source_col, None, dry_run=True)
    elif args.full:
        clean_col = db[CLEAN_COLLECTION]
        clean_col.drop()
        print(f"   '{CLEAN_COLLECTION}' reset (full mode)")
        clean_col = ensure_indexes(db[CLEAN_COLLECTION])
        run(source_col, clean_col)
    else:
        clean_col = ensure_indexes(db[CLEAN_COLLECTION])
        run(source_col, clean_col)
        if args.sample > 0:
            show_sample(clean_col, args.sample)
        print(f"\n   Done! '{CLEAN_COLLECTION}': {clean_col.count_documents({})} total docs")

    client.close()


if __name__ == "__main__":
    main()
