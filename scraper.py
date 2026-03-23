"""
PropertyFinder.eg - Scraper Complet Location → MongoDB
Tous types résidentiels + commerciaux (sauf terrain/land)
Config via .env — HEADLESS mode for server deployment

Prérequis:
    pip install playwright beautifulsoup4 pymongo python-dotenv
    playwright install chromium
"""

import os
import asyncio
import json
import re
import random
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from storage import upload_image_from_bytes, _b2_configured
import logging
log = logging.getLogger("scraper")

load_dotenv()

# ─── CONFIG — from .env ──────────────────────────────────────────
# All rental types — Egypt-wide searches ordered by most recent
# Land (t=5, c=4) excluded — terrain/land
_BASE = "https://www.propertyfinder.eg/en/search?fu=0&ob=mr&rp=m"
ZONES = [
    # ── Residential (c=2) ─────────────────────────────────────────
    ("Apartments",              f"{_BASE}&c=2&t=1"),
    ("Villas",                  f"{_BASE}&c=2&t=35"),
    ("Duplexes",                f"{_BASE}&c=2&t=24"),
    ("Penthouses",              f"{_BASE}&c=2&t=20"),
    ("Townhouses",              f"{_BASE}&c=2&t=22"),
    ("Twin Houses",             f"{_BASE}&c=2&t=46"),
    ("iVillas",                 f"{_BASE}&c=2&t=50"),
    ("Chalets",                 f"{_BASE}&c=2&t=44"),
    ("Roofs",                   f"{_BASE}&c=2&t=53"),
    ("Hotel Apartments",        f"{_BASE}&c=2&t=28"),
    ("Whole Buildings Res.",    f"{_BASE}&c=2&t=10"),
    ("Bungalows",               f"{_BASE}&c=2&t=31"),
    ("Palaces",                 f"{_BASE}&c=2&t=52"),
    ("Cabins",                  f"{_BASE}&c=2&t=51"),
    ("Bulk Rent Units Res.",    f"{_BASE}&c=2&t=34"),
    ("Half Floors Res.",        f"{_BASE}&c=2&t=29"),
    # ── Commercial (c=4) ──────────────────────────────────────────
    ("Offices",                 f"{_BASE}&c=4&t=4"),
    ("Shops",                   f"{_BASE}&c=4&t=21"),
    ("Clinics",                 f"{_BASE}&c=4&t=54"),
    ("Whole Buildings Com.",    f"{_BASE}&c=4&t=10"),
    ("Retail Spaces",           f"{_BASE}&c=4&t=27"),
    ("Full Floors",             f"{_BASE}&c=4&t=18"),
    ("Co-working Spaces",       f"{_BASE}&c=4&t=56"),
    ("Warehouses",              f"{_BASE}&c=4&t=13"),
    ("Villas Com.",             f"{_BASE}&c=4&t=35"),
    ("Factories",               f"{_BASE}&c=4&t=45"),
    ("Half Floors Com.",        f"{_BASE}&c=4&t=29"),
    ("Restaurants",             f"{_BASE}&c=4&t=48"),
    ("Showrooms",               f"{_BASE}&c=4&t=12"),
    ("Cafeterias",              f"{_BASE}&c=4&t=55"),
    ("Bulk Rent Units Com.",    f"{_BASE}&c=4&t=34"),
    ("Medical Facilities",      f"{_BASE}&c=4&t=47"),
    ("Staff Accommodation",     f"{_BASE}&c=4&t=43"),
    ("iVillas Com.",            f"{_BASE}&c=4&t=50"),
]
MAX_PAGES_PER_ZONE = 1000
DELAY_SEARCH = (2, 5)
DELAY_DETAIL = (1.5, 4)
BATCH_SIZE = 10

MONGO_URI = os.environ["MONGODB_URI"]
MONGO_DB = os.getenv("MONGO_PROPERTYFINDER_DB", "propertyfinder")
MONGO_COLLECTION = os.getenv("MONGO_PROPERTYFINDER_COL", "locations")


# ══════════════════════════════════════════════════════════════════
# MONGODB
# ══════════════════════════════════════════════════════════════════

def get_mongo():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print("✅ Connecté à MongoDB")
    col = client[MONGO_DB][MONGO_COLLECTION]
    col.create_index("property_id", unique=True, sparse=True)
    col.create_index("url", unique=True, sparse=True)
    col.create_index("reference", unique=True, sparse=True)
    col.create_index([("city", 1), ("price_value", 1)])
    col.create_index("scraped_at")
    print("✅ Index créés")
    return client, col


def save_batch(col, items: list[dict]) -> dict:
    if not items:
        return {"inserted": 0, "updated": 0}
    now = datetime.now(timezone.utc)
    ops = []
    for li in items:
        if li.get("property_id"):
            f = {"property_id": li["property_id"]}
        elif li.get("reference"):
            f = {"reference": li["reference"]}
        elif li.get("url"):
            f = {"url": li["url"]}
        else:
            continue
        set_doc = {k: v for k, v in li.items() if k not in ("first_seen", "scraped_at")}
        set_doc["updated_at"] = now
        ops.append(UpdateOne(
            f,
            {"$set": set_doc, "$setOnInsert": {"first_seen": now, "scraped_at": now}},
            upsert=True,
        ))
    if not ops:
        return {"inserted": 0, "updated": 0}
    r = col.bulk_write(ops, ordered=False)
    return {"inserted": r.upserted_count, "updated": r.modified_count}


def get_scraped_urls(col) -> set:
    return {d["url"] for d in col.find({"description": {"$exists": True, "$ne": None}}, {"url": 1, "_id": 0}) if d.get("url")}


# ══════════════════════════════════════════════════════════════════
# BROWSER — HEADLESS for server
# ══════════════════════════════════════════════════════════════════

async def make_browser(pw):
    browser = await pw.chromium.launch(
        headless=True,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-setuid-sandbox",
            "--disable-extensions",
            "--disable-background-networking",
            "--disable-default-apps",
            "--disable-sync",
            "--no-first-run",
        ],
    )
    ctx = await browser.new_context(
        viewport={"width": 1920, "height": 1080},
        locale="en-EG", timezone_id="Africa/Cairo",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    )
    await ctx.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-EG', 'en', 'ar'] });
        window.chrome = { runtime: {} };
    """)
    return browser, ctx


async def scroll(page):
    for _ in range(random.randint(3, 5)):
        await page.mouse.wheel(0, random.randint(200, 500))
        await asyncio.sleep(random.uniform(0.3, 0.8))


async def goto(page, url, timeout=30000):
    for attempt in range(3):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            await asyncio.sleep(2)
            html = await page.content()
            if any(kw in html.lower()[:3000] for kw in ["just a moment", "captcha", "cf-challenge"]):
                print("\n  ⚠️ Cloudflare detected — waiting for auto-resolve...")
                for _ in range(180):
                    await asyncio.sleep(1)
                    html = await page.content()
                    if "EGP" in html or "apartments" in html.lower()[3000:8000]:
                        break
                print("  ✅ Cloudflare resolved")
                await asyncio.sleep(2)
            return True
        except Exception as e:
            if attempt < 2:
                print(f"  ⚠️ Retry {attempt+1}: {e}")
                await asyncio.sleep(5)
            else:
                print(f"  ❌ Navigation failed: {e}")
                return False
    return False


# ══════════════════════════════════════════════════════════════════
# EXTRACTION : PAGE DE RECHERCHE → URLs
# ══════════════════════════════════════════════════════════════════

def get_urls_from_search(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    seen, urls = set(), []
    # Match any rental detail page — ends with numeric property ID
    for a in soup.find_all("a", href=re.compile(r"/plp/rent/[^?#]+-\d+\.html")):
        href = a.get("href", "")
        full = href if href.startswith("http") else f"https://www.propertyfinder.eg{href}"
        if full not in seen:
            seen.add(full)
            urls.append(full)
    return urls


# ══════════════════════════════════════════════════════════════════
# EXTRACTION : PAGE DE DÉTAIL → DOCUMENT COMPLET
# ══════════════════════════════════════════════════════════════════

def parse_detail(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # ── 1. __NEXT_DATA__ (primary source — most complete) ─────────────
    prop = {}
    nd_tag = soup.find("script", id="__NEXT_DATA__")
    if nd_tag and nd_tag.string:
        try:
            nd = json.loads(nd_tag.string)
            prop = (nd.get("props", {})
                      .get("pageProps", {})
                      .get("propertyResult", {})
                      .get("property", {}))
        except Exception:
            pass

    # ── 2. JSON-LD #plp-schema (description, amenities, geo, agent) ───
    ld_main = {}
    ld_tag = soup.find("script", id="plp-schema")
    if ld_tag and ld_tag.string:
        try:
            ld = json.loads(ld_tag.string)
            ld_main = ld.get("mainEntity", {}).get("mainEntity", {})
        except Exception:
            pass

    # ── property_id ────────────────────────────────────────────────────
    prop_id = str(prop["id"]) if prop.get("id") else None
    if not prop_id:
        m = re.search(r"-(\d+)\.html$", url)
        prop_id = m.group(1) if m else None

    # ── title ──────────────────────────────────────────────────────────
    title = prop.get("title") or ld_main.get("name")
    if not title:
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else None

    # ── description (JSON-LD always has the real one) ──────────────────
    description = ld_main.get("description") or prop.get("description")

    # ── price ──────────────────────────────────────────────────────────
    price_data   = prop.get("price", {})
    price_value  = price_data.get("value")
    price_currency = price_data.get("currency", "EGP")
    price_period = (price_data.get("period") or "monthly").lower()

    # Annual → monthly normalization
    if price_period in ("yearly", "annual", "year") and price_value:
        price_value  = round(price_value / 12)
        price_period = "monthly"

    price_raw = f"{price_value} {price_currency}/{price_period}" if price_value else None

    # ── property type ──────────────────────────────────────────────────
    property_type = prop.get("property_type")

    # ── area ───────────────────────────────────────────────────────────
    area_sqm = prop.get("area") or prop.get("size")
    if not area_sqm:
        fs = ld_main.get("floorSize", {})
        if str(fs.get("unitText", "")).lower() == "sqm":
            area_sqm = fs.get("value")

    # ── bedrooms / bathrooms ───────────────────────────────────────────
    bedrooms  = prop.get("bedrooms")  or prop.get("bedroom_count")
    bathrooms = prop.get("bathrooms") or prop.get("bathroom_count")
    if bedrooms is None or bathrooms is None:
        text = soup.get_text(" ", strip=True)
        if bedrooms is None:
            bm = re.search(r"Bedrooms?\s*(\d+)", text)
            if bm:
                bedrooms = int(bm.group(1))
        if bathrooms is None:
            btm = re.search(r"Bathrooms?\s*(\d+)", text)
            if btm:
                bathrooms = int(btm.group(1))

    # ── location ───────────────────────────────────────────────────────
    loc = prop.get("location", {})
    location_full = loc.get("full_name") or ld_main.get("address", {}).get("name")

    # path_name: "القاهرة, مدينة القاهرة الجديدة, الرحاب, الرحاب المرحلة الأولى"
    path_parts = [p.strip() for p in loc.get("path_name", "").split(",") if p.strip()]
    city     = path_parts[0] if len(path_parts) > 0 else None
    district = path_parts[1] if len(path_parts) > 1 else None
    compound = path_parts[2] if len(path_parts) > 2 else None

    # ── coordinates ────────────────────────────────────────────────────
    coords    = loc.get("coordinates", {})
    latitude  = coords.get("lat")
    longitude = coords.get("lon")
    if latitude is None:
        geo = ld_main.get("geo", {})
        latitude  = geo.get("latitude")
        longitude = geo.get("longitude")

    # ── images (full resolution from structured data) ──────────────────
    images = []
    for img in prop.get("images", {}).get("property", []):
        img_url = img.get("full") or img.get("medium")
        if img_url and img_url not in images:
            images.append(img_url)
    images = images[:20] or None

    # ── agent / agency (from JSON-LD offer) ────────────────────────────
    agent  = None
    agency = None
    offers = ld_main.get("offers", [])
    if offers:
        offered_by  = offers[0].get("offeredBy", {})
        agent_name  = offered_by.get("name")
        agent_phone = offered_by.get("telephone")
        agency_name = offered_by.get("parentOrganization", {}).get("name")
        if agent_name:
            agent = {"name": agent_name, "phone": agent_phone} if agent_phone else {"name": agent_name}
        if agency_name:
            agency = {"name": agency_name}

    # ── amenities (from JSON-LD amenityFeature) ────────────────────────
    amenities = [f["name"] for f in ld_main.get("amenityFeature", []) if f.get("value")]

    # ── reference ──────────────────────────────────────────────────────
    reference = prop.get("reference")

    # ── furnished ──────────────────────────────────────────────────────
    furnished = prop.get("furnishing") or prop.get("furnished")

    doc = {
        "property_id":   prop_id,
        "reference":     reference,
        "title":         title,
        "description":   description,
        "price":         price_value,
        "currency":      price_currency,
        "price_period":  price_period,
        "price_raw":     price_raw,
        "category":      "rent",
        "property_type": property_type,
        "area_sqm":      area_sqm,
        "bedrooms":      bedrooms,
        "bathrooms":     bathrooms,
        "furnished":     furnished,
        "amenities":     amenities or None,
        "location_full": location_full,
        "city":          city,
        "district":      district,
        "compound":      compound,
        "latitude":      latitude,
        "longitude":     longitude,
        "images":        images,
        "agent":         agent,
        "agency":        agency,
        "url":           url,
        "source":        "propertyfinder.eg",
    }
    return {k: v for k, v in doc.items() if v is not None}


# ══════════════════════════════════════════════════════════════════
# SCRAPER
# ══════════════════════════════════════════════════════════════════

async def scrape_one_listing(page, url: str) -> dict | None:
    # Intercept images as the browser loads them — browser has CDN session/cookies
    captured = {}  # image_uuid -> (bytes, content_type)

    async def _capture_image(route, request):
        try:
            response = await route.fetch()
            if response.ok:
                body = await response.body()
                ct = (response.headers.get("content-type") or "image/jpeg").split(";")[0]
                # URL format: .../listing/{LISTING_ID}/{IMAGE_UUID}/{RESOLUTION}.jpg
                parts = request.url.rstrip("/").split("/")
                uuid = parts[-2] if len(parts) >= 2 else ""
                if uuid and uuid not in captured:
                    captured[uuid] = (body, ct)
            await route.fulfill(response=response)
        except Exception:
            await route.continue_()

    await page.route(
        lambda u: "static.shared.propertyfinder.eg" in u and "/listing/" in u,
        _capture_image,
    )

    if not await goto(page, url):
        await page.unroute_all()
        return None
    try:
        await page.wait_for_selector("h1", timeout=10000)
    except Exception:
        await asyncio.sleep(3)
    await scroll(page)
    html = await page.content()
    await page.unroute_all()

    if "EGP" not in html:
        return None

    doc = parse_detail(html, url)
    if doc and doc.get("images") and _b2_configured():
        prop_id = doc.get("property_id") or url
        uploaded = []
        for i, img_url in enumerate(doc["images"]):
            parts = img_url.rstrip("/").split("/")
            uuid = parts[-2] if len(parts) >= 2 else ""
            item = captured.get(uuid)
            if item:
                data, ct = item
                b2_url = upload_image_from_bytes("propertyfinder", prop_id, img_url, data, ct, i)
                uploaded.append(b2_url)
            else:
                log.warning(f"Image not captured for {img_url[:80]}")
                uploaded.append(img_url)
        doc["images"] = uploaded or None
    return doc


async def scrape_zone(page, zone_name: str, zone_url: str, col, scraped: set, stats: dict):
    print(f"\n{'─'*60}")
    print(f"🌍 ZONE: {zone_name}")
    print(f"   {zone_url}")
    print(f"{'─'*60}")

    if not await goto(page, zone_url):
        print("   ❌ Skip zone")
        return

    zone_scraped = 0
    batch = []
    consecutive_empty = 0

    for pg in range(1, MAX_PAGES_PER_ZONE + 1):
        try:
            await page.wait_for_selector("a[href*='/plp/rent/']", timeout=15000)
        except Exception:
            await asyncio.sleep(3)

        await scroll(page)
        await asyncio.sleep(1)

        html = await page.content()

        if pg == 1:
            tm = re.search(r"([\d,]+)\s*propert", html)
            print(f"   📊 ~{tm.group(1) if tm else '?'} annonces dans cette zone")

        urls = get_urls_from_search(html)
        new_urls = [u for u in urls if u not in scraped]
        skip_count = len(urls) - len(new_urls)

        print(f"\n   📖 Page {pg} | {len(urls)} annonces | {len(new_urls)} nouvelles, {skip_count} déjà en base")

        if len(new_urls) == 0:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                print(f"   🏁 3 pages sans nouvelles annonces → zone terminée")
                break
        else:
            consecutive_empty = 0

        for j, listing_url in enumerate(new_urls, 1):
            print(f"      [{j}/{len(new_urls)}] {listing_url.split('/')[-1][:60]}", end="")

            doc = await scrape_one_listing(page, listing_url)

            if doc:
                batch.append(doc)
                scraped.add(listing_url)
                zone_scraped += 1
                print(f" ✅ {doc.get('price_raw','?')} | {doc.get('bedrooms','?')}ch | {doc.get('city','?')}")
            else:
                print(" ❌ skip")

            if len(batch) >= BATCH_SIZE:
                s = save_batch(col, batch)
                stats["inserted"] += s["inserted"]
                stats["updated"] += s["updated"]
                print(f"      💾 +{s['inserted']} insérés, {s['updated']} maj")
                batch = []

            await asyncio.sleep(random.uniform(*DELAY_DETAIL))

        # Keep all query params, just add/replace page number
        base_no_page = re.sub(r"&page=\d+", "", zone_url)
        next_search = f"{base_no_page}&page={pg + 1}"

        await asyncio.sleep(random.uniform(*DELAY_SEARCH))

        if not await goto(page, next_search):
            print(f"   ❌ Navigation page {pg+1} échouée → zone terminée")
            break

        t = await page.title()
        if "404" in t:
            print(f"   🏁 Page 404 → zone terminée")
            break

    if batch:
        s = save_batch(col, batch)
        stats["inserted"] += s["inserted"]
        stats["updated"] += s["updated"]

    print(f"\n   ✅ Zone '{zone_name}' terminée: {zone_scraped} scrapées")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

async def main():
    mongo_client, col = get_mongo()
    stats = {"inserted": 0, "updated": 0}

    scraped = get_scraped_urls(col)
    print(f"📦 {len(scraped)} annonces déjà complètes en base")
    print(f"🗺️  {len(ZONES)} zones à scraper\n")

    try:
        async with async_playwright() as pw:
            browser, ctx = await make_browser(pw)
            page = await ctx.new_page()

            for i, (name, url) in enumerate(ZONES, 1):
                print(f"\n{'═'*60}")
                print(f"  [{i}/{len(ZONES)}] Lancement zone: {name}")
                print(f"{'═'*60}")

                await scrape_zone(page, name, url, col, scraped, stats)

                scraped = get_scraped_urls(col)
                print(f"\n   📦 Total en base: {col.count_documents({})}")

            await browser.close()

    except KeyboardInterrupt:
        print("\n\n⚠️ Ctrl+C — Sauvegarde en cours...")
    except Exception as e:
        print(f"\n❌ Erreur: {e}")
        import traceback
        traceback.print_exc()
    finally:
        total = col.count_documents({}) if mongo_client else "?"
        print(f"\n{'═'*60}")
        print(f"📊 RÉSULTATS FINAUX")
        print(f"{'═'*60}")
        print(f"   Insérés   : {stats['inserted']}")
        print(f"   Mis à jour: {stats['updated']}")
        print(f"   Total en base: {total}")
        print(f"{'═'*60}")
        mongo_client.close()
        print("🔌 MongoDB déconnecté")


if __name__ == "__main__":
    asyncio.run(main())