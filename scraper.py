"""
PropertyFinder.eg - Scraper Complet Appartements Location → MongoDB
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

load_dotenv()

# ─── CONFIG — from .env ──────────────────────────────────────────
ZONES = [
    ("New Cairo City", "https://www.propertyfinder.eg/en/rent/cairo/apartments-for-rent-new-cairo-city.html"),
    ("Maadi", "https://www.propertyfinder.eg/en/rent/cairo/apartments-for-rent-hay-el-maadi.html"),
    ("Nasr City", "https://www.propertyfinder.eg/en/rent/cairo/apartments-for-rent-nasr-city.html"),
    ("Heliopolis", "https://www.propertyfinder.eg/en/rent/cairo/apartments-for-rent-heliopolis.html"),
    ("Zamalek", "https://www.propertyfinder.eg/en/rent/cairo/apartments-for-rent-zamalek.html"),
    ("Mokattam", "https://www.propertyfinder.eg/en/rent/cairo/apartments-for-rent-mokattam.html"),
    ("Shorouk City", "https://www.propertyfinder.eg/en/rent/cairo/apartments-for-rent-shorouk-city.html"),
    ("El Obour City", "https://www.propertyfinder.eg/en/rent/cairo/apartments-for-rent-el-obour-city.html"),
    ("Cairo Other", "https://www.propertyfinder.eg/en/rent/cairo/apartments-for-rent.html"),
    ("Sheikh Zayed", "https://www.propertyfinder.eg/en/rent/giza/apartments-for-rent-sheikh-zayed-city.html"),
    ("6 October", "https://www.propertyfinder.eg/en/rent/giza/apartments-for-rent-6-october-city.html"),
    ("Giza Other", "https://www.propertyfinder.eg/en/rent/giza/apartments-for-rent.html"),
    ("Alexandria", "https://www.propertyfinder.eg/en/rent/alexandria/apartments-for-rent.html"),
    ("Al Daqahlya", "https://www.propertyfinder.eg/en/rent/al-daqahlya/apartments-for-rent.html"),
    ("Red Sea", "https://www.propertyfinder.eg/en/rent/red-sea/apartments-for-rent.html"),
    ("North Coast", "https://www.propertyfinder.eg/en/rent/north-coast/apartments-for-rent.html"),
    ("Suez", "https://www.propertyfinder.eg/en/rent/suez/apartments-for-rent.html"),
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
        li["updated_at"] = now
        li["type"] = "appartement"
        li["category"] = "location"
        if li.get("property_id"):
            f = {"property_id": li["property_id"]}
        elif li.get("reference"):
            f = {"reference": li["reference"]}
        elif li.get("url"):
            f = {"url": li["url"]}
        else:
            continue
        ops.append(UpdateOne(f, {"$set": li, "$setOnInsert": {"first_seen": now, "scraped_at": now}}, upsert=True))
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
    for a in soup.find_all("a", href=re.compile(r"/plp/rent/apartment-")):
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
    text = soup.get_text(" ", strip=True)

    prop_id = None
    m = re.search(r"-([A-Za-z0-9]{5,})\.html$", url)
    if m:
        prop_id = m.group(1)

    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else None

    price_raw, price_value, price_period = None, None, "monthly"
    pm = re.search(r"EGP\s*([\d,]+)/month", text) or re.search(r"([\d,]+)\s*EGP/month", text)
    if pm:
        price_raw = pm.group(0)
        try:
            price_value = int(pm.group(1).replace(",", ""))
        except ValueError:
            pass
    if not price_value:
        py = re.search(r"([\d,]+)\s*EGP/year", text)
        if py:
            price_raw, price_period = py.group(0), "yearly"
            try:
                price_value = int(py.group(1).replace(",", ""))
            except ValueError:
                pass

    description = None
    if h1:
        parts = []
        for sib in h1.find_all_next():
            if sib.name in ("hr", "table") or sib.get_text(strip=True).startswith("Property details"):
                break
            if sib.name == "p" or (sib.name == "div" and len(sib.get_text(strip=True)) > 30):
                t = sib.get_text("\n", strip=True)
                if t and t != title and "See full description" not in t:
                    parts.append(t)
        if parts:
            description = "\n".join(parts)
    if not description:
        for el in soup.find_all(["p", "div"]):
            t = el.get_text(strip=True)
            if len(t) > 100 and any(kw in t.lower() for kw in ["rent", "bedroom", "sqm", "sq m"]):
                description = t
                break

    property_type = None
    ptm = re.search(r"Property Type\s*(\w[\w\s]*?)(?=Property Size|Bedrooms|$)", text)
    if ptm:
        property_type = ptm.group(1).strip()

    property_size = None
    sm = re.search(r"([\d,]+)\s*sqft\s*/\s*([\d,]+)\s*sqm", text)
    if sm:
        property_size = {"sqft": int(sm.group(1).replace(",", "")), "sqm": int(sm.group(2).replace(",", ""))}
    else:
        sm2 = re.search(r"([\d,]+)\s*sqm", text)
        if sm2:
            property_size = {"sqm": int(sm2.group(1).replace(",", ""))}

    bedrooms = None
    bm = re.search(r"Bedrooms?\s*(\d+)", text)
    if bm:
        bedrooms = int(bm.group(1))

    bathrooms = None
    btm = re.search(r"Bathrooms?\s*(\d+)", text)
    if btm:
        bathrooms = int(btm.group(1))

    available_from = None
    am = re.search(r"Available from\s*(\d{1,2}\s\w+\s\d{4})", text)
    if am:
        available_from = am.group(1)

    amenities = []
    asec = soup.find(string=re.compile(r"Amenities"))
    if asec:
        container = asec.find_parent()
        if container:
            for _ in range(5):
                if container.parent:
                    container = container.parent
                    ct = [c.get_text(strip=True) for c in container.find_all(["span", "li", "div"]) if 2 < len(c.get_text(strip=True)) < 50]
                    if len(ct) >= 3:
                        amenities = [t for t in ct if t not in ("Amenities",) and not t.startswith("See all")]
                        amenities = list(dict.fromkeys(amenities))
                        break
    if not amenities:
        known = ["Furnished", "Balcony", "Built in Wardrobes", "Central A/C", "Covered Parking",
                 "Kitchen Appliances", "Private Garden", "Study", "Shared Spa", "Security",
                 "Swimming Pool", "Gym", "Elevator", "Maid's Room", "Storage Room", "Pets Allowed",
                 "Concierge", "Children's Play Area", "BBQ Area", "Jacuzzi", "Sauna", "Steam Room",
                 "View of Landmark", "View of Water", "Shared Pool", "Private Pool", "Internet"]
        for a in known:
            if a.lower() in text.lower():
                amenities.append(a)

    location_full, city, district, compound = None, None, None, None
    lm = re.search(
        r"([A-Z][\w\s\.]+(?:,\s*[A-Z][\w\s\.]+){1,5},\s*(?:Cairo|Giza|Alexandria|Red Sea|Suez|North Coast|South Sainai|Al Daqahlya|Qalyubia|Asyut))",
        text,
    )
    if lm:
        location_full = lm.group(1).strip()
        parts = [p.strip() for p in location_full.split(",")]
        city = parts[-1] if parts else None
        district = parts[-2] if len(parts) >= 2 else None
        compound = parts[0] if len(parts) >= 3 else None

    images = []
    for img in soup.find_all("img", src=re.compile(r"static\.shared\.propertyfinder")):
        src = img.get("src", "")
        if "/listing/" in src:
            hi = re.sub(r"/\d+x\d+\.", "/1200x800.", src)
            if hi not in images:
                images.append(hi)
    images = images[:20]

    agent_name, agency_name = None, None
    prov = soup.find(string=re.compile(r"Provided by"))
    if prov:
        c = prov.find_parent()
        if c:
            for _ in range(3):
                c = c.parent if c.parent else c
            for t in c.stripped_strings:
                t = t.strip()
                if len(t) <= 3 or t in ("Provided by", "Regulatory information") or t.startswith("See agency"):
                    continue
                if not agent_name:
                    agent_name = t
                elif not agency_name and t != agent_name:
                    agency_name = t
                    break

    reference = None
    rm = re.search(r"Reference\s*([A-Z0-9]{10,})", text)
    if rm:
        reference = rm.group(1)

    listed_date = None
    ldm = re.search(r"Listed\s+(.+?)(?:\s*Call|\s*WhatsApp|\s*$)", text)
    if ldm:
        listed_date = ldm.group(1).strip()

    price_insights = {}
    cm = re.search(r"costs?\s*(\d+)%\s*(less|more)", text)
    if cm:
        price_insights["vs_avg_pct"] = int(cm.group(1))
        price_insights["vs_avg_dir"] = cm.group(2)
    arm = re.search(r"Average Rent is\s*([\d,]+)\s*EGP", text)
    if arm:
        price_insights["avg_rent"] = int(arm.group(1).replace(",", ""))
    asm = re.search(r"Average size is\s*(\d+)\s*sqm", text)
    if asm:
        price_insights["avg_size"] = int(asm.group(1))

    doc = {
        "property_id": prop_id, "reference": reference, "title": title,
        "description": description, "price_raw": price_raw, "price_value": price_value,
        "currency": "EGP", "price_period": price_period, "property_type": property_type,
        "property_size": property_size, "bedrooms": bedrooms, "bathrooms": bathrooms,
        "available_from": available_from,
        "furnished": "furnished" if "Furnished" in amenities else None,
        "amenities": amenities or None, "location_full": location_full,
        "city": city, "district": district, "compound": compound,
        "images": images or None, "agent_name": agent_name, "agency_name": agency_name,
        "listed_date": listed_date, "price_insights": price_insights or None,
        "url": url, "source": "propertyfinder.eg",
    }
    return {k: v for k, v in doc.items() if v is not None}


# ══════════════════════════════════════════════════════════════════
# SCRAPER
# ══════════════════════════════════════════════════════════════════

async def scrape_one_listing(page, url: str) -> dict | None:
    if not await goto(page, url):
        return None
    try:
        await page.wait_for_selector("h1", timeout=10000)
    except Exception:
        await asyncio.sleep(3)
    await scroll(page)
    html = await page.content()
    if "EGP" not in html:
        return None
    return parse_detail(html, url)


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
            await page.wait_for_selector("a[href*='/plp/rent/apartment-']", timeout=15000)
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

        search_url_for_next_page = zone_url.split("?")[0]
        next_search = f"{search_url_for_next_page}?page={pg + 1}"

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