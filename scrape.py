"""
Scrape Jemena planned outages, match streets to OpenStreetMap geometry,
identify affected client sites, and rebuild docs/.

Outputs:
    docs/index.html          - interactive map (suburbs + shaded streets + clients)
    docs/affected.html       - sortable table of affected clients
    docs/affected.csv        - same data as CSV
    docs/data.json           - raw payload (also embedded in index.html)
    docs/_last_scrape.*      - debug snapshots, always written

Inputs:
    clients.csv              - your account list (root of repo)
    .cache/chains.json       - pulled-from-OSM chains, refreshed weekly
    .cache/suburbs.json      - geocoded suburb centroids
    .cache/clients.json      - geocoded client coordinates
"""
from __future__ import annotations

import asyncio
import csv
import json
import math
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent
DOCS = ROOT / "docs"
CACHE = ROOT / ".cache"
TEMPLATE = ROOT / "template.html"
AFFECTED_TEMPLATE = ROOT / "affected_template.html"
CLIENTS_CSV = ROOT / "clients.csv"
DOCS.mkdir(exist_ok=True)
CACHE.mkdir(exist_ok=True)

JEMENA_URL = "https://www.jemena.com.au/outages/electricity-outages/planned-outages/"

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
REPO_URL = os.environ.get("REPO_URL", "https://github.com/Nexus-Energy393/jemena-outages")
USER_AGENT = f"jemena-outage-map/2.0 (+{REPO_URL})"
MELBOURNE_TZ = timezone(timedelta(hours=10))

# Chains to pull from OSM. Each entry is an Overpass tag filter and a label.
# Refreshed weekly; cached the rest of the time.
CHAINS = [
    ("McDonald's", '["amenity"="fast_food"]["brand"~"McDonald",i]'),
    ("Hungry Jack's", '["amenity"="fast_food"]["brand"~"Hungry",i]'),
    ("KFC", '["amenity"="fast_food"]["brand"~"^KFC$",i]'),
    ("Aldi", '["shop"="supermarket"]["brand"~"^ALDI$",i]'),
    ("Coles", '["shop"="supermarket"]["brand"~"^Coles$",i]'),
    ("Woolworths", '["shop"="supermarket"]["brand"~"Woolworths",i]'),
    ("IGA", '["shop"="supermarket"]["brand"~"^IGA$",i]'),
    ("Bunnings", '["shop"~"hardware|doityourself"]["brand"~"Bunnings",i]'),
    ("Officeworks", '["shop"]["brand"~"Officeworks",i]'),
    ("Kmart", '["shop"="department_store"]["brand"~"^Kmart$",i]'),
    ("Target", '["shop"="department_store"]["brand"~"^Target$",i]'),
    ("Big W", '["shop"="department_store"]["brand"~"Big W",i]'),
    ("Shopping centre", '["shop"="mall"]'),
]

# Bounding box covering Jemena's electricity service area
# (Melbourne north and west). Tightened a bit beyond the suburb spread
# we've seen, so a new outlier suburb still falls inside.
JEMENA_BBOX = (-37.95, 144.55, -37.40, 145.20)  # south, west, north, east

CHAINS_REFRESH_DAYS = 7
BUFFER_METRES = 200  # "possibly affected" radius around shaded streets

# Default minimum outage duration (hours) to consider a client a generator-hire
# opportunity. Per-client values in clients.csv override this. Set to 0 to
# include every affected client regardless of outage length.
DEFAULT_MIN_HOURS = 4.0


# ---------------------------------------------------------------------------
# Street-name normalisation
# ---------------------------------------------------------------------------
ABBR = {
    "ST": "Street", "RD": "Road", "AVE": "Avenue", "DR": "Drive",
    "CT": "Court", "CRES": "Crescent", "CCT": "Circuit", "WAY": "Way",
    "PL": "Place", "PDE": "Parade", "HWY": "Highway", "BVD": "Boulevard",
    "GR": "Grove", "CL": "Close", "LANE": "Lane", "LOOP": "Loop",
}


def normalise_street(raw: str) -> str:
    s = raw.strip().upper()
    s = re.sub(r"\bSTREET ST\b", "ST", s)
    s = re.sub(r"\bST ST\b", "ST", s)
    parts = s.split()
    if not parts:
        return ""
    out = [
        ABBR[p] if i == len(parts) - 1 and p in ABBR else p.title()
        for i, p in enumerate(parts)
    ]
    return "-".join(w[:1].upper() + w[1:] for w in " ".join(out).split("-"))


def norm_key(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def extract_street_from_address(address: str) -> str:
    """'123 Sydney Rd' -> 'Sydney Road'. Best-effort."""
    if not address:
        return ""
    s = address.strip()
    # Strip leading number/range like '123' or '123-125' or 'Shop 4, 123'
    s = re.sub(r"^(shop|unit|suite)\s*\w+\s*,?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^\d+[a-z]?(\s*[-/]\s*\d+[a-z]?)?\s*", "", s)
    return normalise_street(s)


# ---------------------------------------------------------------------------
# Date / time
# ---------------------------------------------------------------------------
MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}


def parse_date_str(s: str) -> datetime | None:
    m = re.match(r"(\d{1,2})[- ]([A-Za-z]+)", s.strip())
    if not m:
        return None
    day = int(m.group(1))
    month_name = m.group(2)[:3].title()
    if month_name not in MONTHS:
        return None
    month = MONTHS[month_name]
    today = datetime.now(MELBOURNE_TZ).date()
    for year in [today.year, today.year + 1, today.year - 1]:
        try:
            dt = datetime(year, month, day, tzinfo=MELBOURNE_TZ)
        except ValueError:
            continue
        days_ahead = (dt.date() - today).days
        if -30 <= days_ahead <= 200:
            return dt
    try:
        return datetime(today.year, month, day, tzinfo=MELBOURNE_TZ)
    except ValueError:
        return None


def parse_time_range(s: str):
    m = re.match(r"(\d{3,4})\s*[-–]\s*(\d{3,4})", s.strip())
    if not m:
        return None, None

    def hm(chunk: str):
        chunk = chunk.zfill(4)
        return int(chunk[:2]), int(chunk[2:])

    return hm(m.group(1)), hm(m.group(2))


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------
def haversine_m(lat1, lng1, lat2, lng2):
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def point_to_segment_m(plat, plng, alat, alng, blat, blng):
    """Approx distance from point to segment in metres (small-area flat projection)."""
    # Project to local metres using equirectangular at midpoint latitude
    lat_mid = (alat + blat) / 2.0
    cos_mid = math.cos(math.radians(lat_mid))
    mx = (plng - alng) * 111320.0 * cos_mid
    my = (plat - alat) * 110540.0
    bx = (blng - alng) * 111320.0 * cos_mid
    by = (blat - alat) * 110540.0
    bb = bx * bx + by * by
    if bb < 1e-9:
        return math.hypot(mx, my)
    t = max(0.0, min(1.0, (mx * bx + my * by) / bb))
    dx, dy = mx - t * bx, my - t * by
    return math.hypot(dx, dy)


def point_to_polyline_m(plat, plng, coords):
    """Min distance from point (plat, plng) to a polyline given as [[lat,lng],...]."""
    best = math.inf
    for (a, b) in zip(coords, coords[1:]):
        d = point_to_segment_m(plat, plng, a[0], a[1], b[0], b[1])
        if d < best:
            best = d
    return best


def polyline_bbox(coords):
    lats = [c[0] for c in coords]
    lngs = [c[1] for c in coords]
    return min(lats), min(lngs), max(lats), max(lngs)


def bbox_expand(bbox, margin_m):
    s, w, n, e = bbox
    dlat = margin_m / 110540.0
    dlng = margin_m / (111320.0 * math.cos(math.radians((s + n) / 2)))
    return s - dlat, w - dlng, n + dlat, e + dlng


def point_in_bbox(plat, plng, bbox):
    s, w, n, e = bbox
    return s <= plat <= n and w <= plng <= e


# ---------------------------------------------------------------------------
# Scraping (unchanged from v1.1)
# ---------------------------------------------------------------------------
async def scrape_outages():
    from playwright.async_api import async_playwright

    print(f"[scrape] loading {JEMENA_URL}", flush=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1400, "height": 2400},
        )
        page = await ctx.new_page()
        try:
            await page.goto(JEMENA_URL, wait_until="domcontentloaded", timeout=90000)
            try:
                await page.wait_for_load_state("networkidle", timeout=30000)
            except Exception:
                pass
            await page.wait_for_timeout(3000)

            for sel in [
                'button:has-text("Accept")',
                'button:has-text("Accept all")',
                'button:has-text("Allow")',
            ]:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=500):
                        await btn.click()
                        await page.wait_for_timeout(500)
                        break
                except Exception:
                    pass

            try:
                await page.wait_for_selector("table tbody tr", timeout=30000)
            except Exception:
                pass

            clicked = await page.evaluate(
                """() => {
                    const headers = document.querySelectorAll('tr.cursor-pointer');
                    headers.forEach(h => h.click());
                    return headers.length;
                }"""
            )
            print(f"[scrape] clicked {clicked} suburb headers", flush=True)
            await page.wait_for_timeout(2000)

            try:
                await page.screenshot(path=str(DOCS / "_last_scrape.png"), full_page=True, timeout=30000)
            except Exception:
                pass
            try:
                (DOCS / "_last_scrape.html").write_text(await page.content(), encoding="utf-8")
            except Exception:
                pass

            rows = await page.evaluate(
                """() => {
                    const tables = [...document.querySelectorAll('table')];
                    let bestRows = null, bestCount = 0;
                    for (const t of tables) {
                        const trs = [...t.querySelectorAll('tr')];
                        const cells = trs.map(tr => {
                            const tds = [...tr.querySelectorAll('td,th')];
                            return tds.map(td => ({
                                text: (td.innerText || '').trim(),
                                colspan: parseInt(td.getAttribute('colspan') || '1', 10)
                            }));
                        });
                        const dataRows = cells.filter(r => r.length >= 6).length;
                        if (dataRows > bestCount) { bestCount = dataRows; bestRows = cells; }
                    }
                    return bestRows || [];
                }"""
            )
            try:
                (DOCS / "_last_scrape_raw.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
            except Exception:
                pass
        finally:
            await browser.close()

    if not rows:
        raise RuntimeError("No table found on Jemena page.")
    outages = _parse_table(rows)
    if not outages:
        raise RuntimeError("Found rows but no parseable outage data.")
    return outages


def _parse_table(rows):
    out = []
    for row in rows:
        if len(row) < 6:
            continue
        cells = [c["text"] for c in row[:6]]
        suburb_text, street, date_str, day_str, time_str, status = cells
        if not suburb_text or suburb_text.strip().lower() in {"suburb", "location"}:
            continue
        date_dt = parse_date_str(date_str)
        if date_dt is None:
            continue
        sh, eh = parse_time_range(time_str)
        if sh is None or eh is None:
            continue
        start_dt = date_dt.replace(hour=sh[0], minute=sh[1])
        end_dt = date_dt.replace(hour=eh[0], minute=eh[1])
        if end_dt <= start_dt:
            end_dt = end_dt + timedelta(days=1)
        out.append({
            "suburb": suburb_text.strip().upper(),
            "street_raw": street.strip(),
            "street": normalise_street(street),
            "start_dt": start_dt,
            "end_dt": end_dt,
            "start_display": start_dt.strftime("%a %d %b, %I:%M %p").replace(" 0", " "),
            "end_display": end_dt.strftime("%I:%M %p").lstrip("0"),
            "status": (status.strip() or "Scheduled"),
        })
    return out


# ---------------------------------------------------------------------------
# Caches
# ---------------------------------------------------------------------------
def load_cache(name):
    f = CACHE / name
    if f.exists():
        try:
            return json.loads(f.read_text())
        except Exception:
            pass
    return {}


def save_cache(name, data):
    (CACHE / name).write_text(json.dumps(data, indent=2, sort_keys=True))


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------
def nominatim(query: str, retries=2):
    for attempt in range(retries + 1):
        try:
            r = requests.get(
                NOMINATIM_URL,
                params={"q": query, "format": "json", "limit": 1, "countrycodes": "au"},
                headers={"User-Agent": USER_AGENT, "Accept-Language": "en-AU"},
                timeout=30,
            )
            r.raise_for_status()
            j = r.json()
            time.sleep(1.2)
            return j[0] if j else None
        except Exception as e:
            print(f"[nominatim] {query!r} attempt {attempt+1} failed: {e}", flush=True)
            time.sleep(2)
    return None


def geocode_suburb(name):
    cache = load_cache("suburbs.json")
    if name.upper() in cache:
        return cache[name.upper()]
    print(f"[geocode-suburb] {name}", flush=True)
    r = nominatim(f"{name}, Victoria, Australia")
    if not r:
        return None
    loc = {"lat": float(r["lat"]), "lng": float(r["lon"])}
    cache[name.upper()] = loc
    save_cache("suburbs.json", cache)
    return loc


def geocode_client(client):
    """client: dict with name/address/suburb/postcode. Returns dict with lat/lng added, or None."""
    cache = load_cache("clients.json")
    key = f"{client.get('address','')}|{client.get('suburb','')}|{client.get('postcode','')}|{client.get('name','')}".upper()
    if key in cache:
        c = dict(client)
        c.update(cache[key])
        return c
    addr_parts = [client.get("address", ""), client.get("suburb", ""), client.get("postcode", ""), "Victoria, Australia"]
    query = ", ".join(p for p in addr_parts if p)
    print(f"[geocode-client] {client.get('name')}: {query}", flush=True)
    r = nominatim(query)
    if not r:
        # Fall back to suburb only
        sub_loc = geocode_suburb(client.get("suburb", ""))
        if sub_loc:
            cache[key] = {"lat": sub_loc["lat"], "lng": sub_loc["lng"], "geocoded": "suburb-fallback"}
            save_cache("clients.json", cache)
            c = dict(client)
            c.update(cache[key])
            return c
        return None
    loc = {"lat": float(r["lat"]), "lng": float(r["lon"]), "geocoded": "address"}
    cache[key] = loc
    save_cache("clients.json", cache)
    c = dict(client)
    c.update(loc)
    return c


# ---------------------------------------------------------------------------
# Overpass
# ---------------------------------------------------------------------------
def regex_escape_minimal(s):
    return re.sub(r"([.^$*+?()\[\]{}|\\])", r"\\\1", s)


def fetch_overpass(query: str):
    last_err = None
    for ep in OVERPASS_ENDPOINTS:
        try:
            print(f"[overpass] trying {ep}", flush=True)
            r = requests.post(ep, data={"data": query}, headers={"User-Agent": USER_AGENT}, timeout=300)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"[overpass] {ep} failed: {e}", flush=True)
            last_err = e
    raise RuntimeError(f"Overpass failed: {last_err}")


def build_streets_query(suburb_streets, suburb_geo):
    lines = ["[out:json][timeout:180];", "("]
    for suburb in sorted(suburb_streets):
        g = suburb_geo.get(suburb)
        if not g:
            continue
        names = "|".join(regex_escape_minimal(s) for s in sorted(suburb_streets[suburb]))
        lines.append(f'  way["name"~"^({names})$",i]["highway"](around:3500,{g["lat"]},{g["lng"]});')
    lines += [");", "out geom;"]
    return "\n".join(lines)


def build_chains_query(bbox):
    s, w, n, e = bbox
    parts = []
    for label, filt in CHAINS:
        parts.append(f'  node{filt}({s},{w},{n},{e});')
        parts.append(f'  way{filt}({s},{w},{n},{e});')
    return "[out:json][timeout:120];\n(\n" + "\n".join(parts) + "\n);\nout center tags;"


def fetch_chains():
    """Pull chain stores from OSM, with weekly cache."""
    meta = load_cache("chains_meta.json")
    last = meta.get("last_fetch")
    if last:
        try:
            last_dt = datetime.fromisoformat(last)
            if (datetime.now(timezone.utc) - last_dt) < timedelta(days=CHAINS_REFRESH_DAYS):
                cached = load_cache("chains.json")
                if cached.get("clients"):
                    print(f"[chains] using cache from {last}", flush=True)
                    return cached["clients"]
        except Exception:
            pass

    print("[chains] fetching from OSM (weekly refresh)", flush=True)
    query = build_chains_query(JEMENA_BBOX)
    try:
        result = fetch_overpass(query)
    except Exception as e:
        print(f"[chains] fetch failed, using stale cache if any: {e}", flush=True)
        cached = load_cache("chains.json")
        return cached.get("clients", [])

    clients = []
    seen = set()
    for el in result.get("elements", []):
        tags = el.get("tags") or {}
        # Pick a label
        label = None
        for ll, _ in CHAINS:
            brand = (tags.get("brand") or "").lower()
            name_l = (tags.get("name") or "").lower()
            if ll.lower().rstrip("'s").rstrip("s") in brand or ll.lower().rstrip("'s").rstrip("s") in name_l:
                label = ll
                break
            if ll == "Shopping centre" and tags.get("shop") == "mall":
                label = ll
                break
        if not label:
            continue

        if el.get("type") == "node":
            lat, lng = el.get("lat"), el.get("lon")
        else:
            c = el.get("center") or {}
            lat, lng = c.get("lat"), c.get("lon")
        if lat is None or lng is None:
            continue

        addr = " ".join(filter(None, [
            tags.get("addr:housenumber"),
            tags.get("addr:street"),
        ]))
        suburb = (tags.get("addr:suburb") or tags.get("addr:city") or "").upper()
        postcode = tags.get("addr:postcode") or ""
        name = tags.get("name") or label

        # Dedupe by rounded coords
        k = (round(lat, 5), round(lng, 5), name)
        if k in seen:
            continue
        seen.add(k)

        clients.append({
            "source": "osm",
            "name": name,
            "category": label,
            "address": addr,
            "suburb": suburb,
            "postcode": postcode,
            "contact_name": "",
            "contact_phone": "",
            "contact_email": "",
            "notes": "",
            "lat": lat,
            "lng": lng,
        })

    save_cache("chains.json", {"clients": clients})
    save_cache("chains_meta.json", {"last_fetch": datetime.now(timezone.utc).isoformat()})
    print(f"[chains] {len(clients)} chain locations cached", flush=True)
    return clients


def match_streets(overpass, outages_by_pair, pairs_by_street, max_dist_deg=0.04):
    max_sq = max_dist_deg * max_dist_deg
    kept = []
    for el in overpass.get("elements", []):
        if el.get("type") != "way":
            continue
        geom = el.get("geometry") or []
        if len(geom) < 2:
            continue
        name = (el.get("tags") or {}).get("name")
        if not name:
            continue
        coords = [[round(p["lat"], 6), round(p["lon"], 6)] for p in geom]
        mid_lat = sum(c[0] for c in coords) / len(coords)
        mid_lng = sum(c[1] for c in coords) / len(coords)
        candidates = pairs_by_street.get(norm_key(name), [])
        if not candidates:
            continue
        best, best_sq = None, math.inf
        for sub_name, slat, slng in candidates:
            d = (slat - mid_lat) ** 2 + (slng - mid_lng) ** 2
            if d < best_sq:
                best_sq = d
                best = sub_name
        if best is None or best_sq > max_sq:
            continue
        outs = outages_by_pair[(best.lower(), norm_key(name))]
        kept.append({
            "name": name,
            "suburb": best,
            "coords": coords,
            "outages": outs,
            "cancelled": all(o["status"].lower() == "cancelled" for o in outs),
        })
    return kept


# ---------------------------------------------------------------------------
# Client list assembly
# ---------------------------------------------------------------------------
def load_user_clients():
    if not CLIENTS_CSV.exists():
        return []
    out = []
    with CLIENTS_CSV.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row = {k: (v or "").strip() for k, v in row.items()}
            if not row.get("name") or not row.get("suburb"):
                continue
            row["source"] = "user"
            out.append(row)
    return out


def dedupe_clients(clients, location_tolerance_m=50):
    """Collapse duplicates that refer to the same physical site.

    Two entries are considered duplicates if they're within
    `location_tolerance_m` of each other AND share the same brand or name
    (case-insensitive). User-provided rows always win over OSM-pulled chains
    because they typically have richer contact info.
    """
    # Sort so user clients are considered first
    sorted_clients = sorted(clients, key=lambda c: (c.get("source") != "user",))

    kept = []
    for c in sorted_clients:
        clat, clng = c.get("lat"), c.get("lng")
        if clat is None or clng is None:
            continue
        cname = (c.get("name") or "").lower()
        ccat = (c.get("category") or "").lower()
        is_dup = False
        for k in kept:
            d = haversine_m(clat, clng, k["lat"], k["lng"])
            if d <= location_tolerance_m:
                kname = (k.get("name") or "").lower()
                kcat = (k.get("category") or "").lower()
                # Same site if names overlap or categories match
                if (cname and kname and (cname in kname or kname in cname)) \
                        or (ccat and ccat == kcat):
                    is_dup = True
                    # Merge any contact info from the duplicate into the kept one
                    for field in ("contact_name", "contact_phone", "contact_email", "address"):
                        if not k.get(field) and c.get(field):
                            k[field] = c[field]
                    break
        if not is_dup:
            kept.append(c)
    return kept


def assemble_clients():
    user_clients = load_user_clients()
    print(f"[clients] {len(user_clients)} from clients.csv", flush=True)

    chain_clients = fetch_chains()
    print(f"[clients] {len(chain_clients)} chains from OSM", flush=True)

    geocoded = []
    for c in user_clients:
        gc = geocode_client(c)
        if gc and gc.get("lat") and gc.get("lng"):
            geocoded.append(gc)
    # Chain clients are already geocoded
    geocoded.extend(chain_clients)

    deduped = dedupe_clients(geocoded)
    if len(deduped) < len(geocoded):
        print(f"[clients] deduped {len(geocoded) - len(deduped)} duplicate sites "
              f"({len(deduped)} unique remaining)", flush=True)
    return deduped


# ---------------------------------------------------------------------------
# Affected-clients matching
# ---------------------------------------------------------------------------
def match_clients_to_outages(clients, streets, raw_outages):
    """For each client, return:
       definite: outage matched by exact (suburb, street) overlap with their address
       possible: client lies within BUFFER_METRES of a shaded street geometry
    """
    # Index outages by (suburb_l, street_n) for definite-check
    outages_by_pair = {}
    for o in raw_outages:
        outages_by_pair.setdefault((o["suburb"].lower(), norm_key(o["street"])), []).append(o)

    # Pre-compute bboxes for each street (with buffer expansion)
    bboxes = []
    for s in streets:
        bbox = polyline_bbox(s["coords"])
        bboxes.append(bbox_expand(bbox, BUFFER_METRES))

    affected = []
    for c in clients:
        plat, plng = c["lat"], c["lng"]
        client_street = extract_street_from_address(c.get("address", ""))
        client_suburb_l = (c.get("suburb") or "").lower()

        definite_outages = []
        possible_outages = []
        nearest_distance = math.inf
        nearest_street_name = None
        nearest_street_suburb = None

        # Definite via address match
        if client_street and client_suburb_l:
            key = (client_suburb_l, norm_key(client_street))
            if key in outages_by_pair:
                definite_outages.extend(outages_by_pair[key])

        # Possible via geometry proximity
        for bbox, s in zip(bboxes, streets):
            if not point_in_bbox(plat, plng, bbox):
                continue
            d = point_to_polyline_m(plat, plng, s["coords"])
            if d < nearest_distance:
                nearest_distance = d
                nearest_street_name = s["name"]
                nearest_street_suburb = s["suburb"]
            if d <= BUFFER_METRES:
                # Avoid double-counting if already in definite
                for o in s["outages"]:
                    sig = (o["suburb"], o["street"], o["start"], o["end"])
                    if sig not in {(d2["suburb"], d2["street"], d2["start"], d2["end"]) for d2 in definite_outages}:
                        possible_outages.append({**o, "_distance_m": int(d)})

        if not definite_outages and not possible_outages:
            continue

        # Dedupe possible outages by signature; keep min distance
        unique_possible = {}
        for o in possible_outages:
            sig = (o["suburb"], o["street"], o["start"], o["end"])
            if sig not in unique_possible or o["_distance_m"] < unique_possible[sig]["_distance_m"]:
                unique_possible[sig] = o

        affected.append({
            "client": c,
            "definite": definite_outages,
            "possible": list(unique_possible.values()),
            "nearest_distance_m": int(nearest_distance) if nearest_distance < math.inf else None,
            "nearest_street": (
                f"{nearest_street_name}, {nearest_street_suburb}"
                if nearest_street_name else None
            ),
        })
    return affected


# ---------------------------------------------------------------------------
# Output rendering
# ---------------------------------------------------------------------------
def render_main_html(payload):
    template = TEMPLATE.read_text(encoding="utf-8")
    return template.replace("__DATA__", json.dumps(payload, separators=(",", ":")))


def render_affected_html(payload):
    template = AFFECTED_TEMPLATE.read_text(encoding="utf-8")
    return template.replace("__DATA__", json.dumps(payload, separators=(",", ":")))


def write_affected_csv(affected, path, default_min_hours):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "Match", "Client", "Category", "Source", "Address", "Suburb",
            "Postcode", "Contact Name", "Contact Phone", "Contact Email",
            "Outage Suburb", "Outage Street", "Start", "End", "Duration (hrs)",
            "Status", "Distance (m)",
            "Client Total Hours", "Client Longest Outage (hrs)",
            "Min Hours Threshold", "Generator Opportunity", "Notes",
        ])
        for a in affected:
            c = a["client"]
            client_min = c.get("min_outage_hours")
            try:
                threshold = float(client_min) if client_min not in (None, "") else default_min_hours
            except (ValueError, TypeError):
                threshold = default_min_hours
            longest = 0.0
            total = 0.0
            for o in a.get("definite", []) + a.get("possible", []):
                if o.get("status", "").lower() == "cancelled":
                    continue
                d = float(o.get("duration_hours") or 0)
                longest = max(longest, d)
                total += d
            opportunity = "Yes" if longest >= threshold else "No"

            for outages, label in (
                (a.get("definite", []), "Definite"),
                (a.get("possible", []), "Possible"),
            ):
                for o in outages:
                    w.writerow([
                        label, c.get("name", ""), c.get("category", ""), c.get("source", ""),
                        c.get("address", ""), c.get("suburb", ""), c.get("postcode", ""),
                        c.get("contact_name", ""), c.get("contact_phone", ""), c.get("contact_email", ""),
                        o["suburb"], o["street"], o["start"], o["end"],
                        o.get("duration_hours", ""), o["status"],
                        o.get("_distance_m", "") if label == "Possible" else "",
                        round(total, 2), round(longest, 2),
                        threshold, opportunity, c.get("notes", ""),
                    ])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    try:
        raw_outages = asyncio.run(scrape_outages())
    except Exception as e:
        print(f"[fatal] scrape failed: {e}", flush=True)
        traceback.print_exc()
        return 1

    print(f"[scrape] got {len(raw_outages)} outages", flush=True)

    suburb_streets = {}
    for o in raw_outages:
        suburb_streets.setdefault(o["suburb"], set()).add(o["street"])
    suburb_streets = {k: sorted(v) for k, v in suburb_streets.items()}

    suburb_geo = {}
    for sub in sorted(suburb_streets):
        loc = geocode_suburb(sub)
        if loc:
            suburb_geo[sub] = loc

    if not suburb_geo:
        print("[fatal] no suburbs geocoded", flush=True)
        return 1

    try:
        overpass = fetch_overpass(build_streets_query(suburb_streets, suburb_geo))
    except Exception as e:
        print(f"[fatal] overpass streets failed: {e}", flush=True)
        return 1

    outages_by_pair = {}
    for o in raw_outages:
        duration_h = (o["end_dt"] - o["start_dt"]).total_seconds() / 3600.0
        outages_by_pair.setdefault((o["suburb"].lower(), norm_key(o["street"])), []).append({
            "suburb": o["suburb"].title(),
            "street": o["street"],
            "start": o["start_display"],
            "end": o["end_display"],
            "status": o["status"],
            "duration_hours": round(duration_h, 2),
        })

    pairs_by_street = {}
    for (suburb_l, street_n), outs in outages_by_pair.items():
        g = suburb_geo.get(suburb_l.upper())
        if g:
            pairs_by_street.setdefault(street_n, []).append((suburb_l.title(), g["lat"], g["lng"]))

    streets = match_streets(overpass, outages_by_pair, pairs_by_street)
    print(f"[match] {len(streets)} street segments matched", flush=True)

    # Clients
    try:
        clients = assemble_clients()
    except Exception as e:
        print(f"[warn] client assembly failed: {e}", flush=True)
        clients = []
    print(f"[clients] {len(clients)} total geocoded", flush=True)

    affected = match_clients_to_outages(clients, streets, raw_outages)
    n_def = sum(1 for a in affected if a["definite"])
    n_pos = sum(1 for a in affected if a["possible"] and not a["definite"])
    print(f"[affected] {n_def} definite, {n_pos} possible-only", flush=True)

    # Suburb summaries
    by_suburb = {}
    for o in raw_outages:
        b = by_suburb.setdefault(o["suburb"], {"name": o["suburb"].title(), "planned": 0, "cancelled": 0, "streets": set()})
        b["streets"].add(o["street"])
        if o["status"].lower() == "cancelled":
            b["cancelled"] += 1
        else:
            b["planned"] += 1
    suburbs = []
    for name, v in by_suburb.items():
        g = suburb_geo.get(name)
        if not g:
            continue
        suburbs.append({
            "name": v["name"], "lat": g["lat"], "lng": g["lng"],
            "planned": v["planned"], "cancelled": v["cancelled"],
            "streets": sorted(v["streets"]),
        })

    matched_pairs = sorted({(s["suburb"].lower(), norm_key(s["name"])) for s in streets})

    now = datetime.now(MELBOURNE_TZ).strftime("%a %d %b %Y, %I:%M %p AEST")

    # Slim down clients for embedding (drop heavy/unused fields)
    def slim_client(c):
        out = {}
        for k in ("name", "category", "source", "address", "suburb", "postcode",
                  "contact_name", "contact_phone", "contact_email", "notes",
                  "lat", "lng"):
            if c.get(k) not in (None, ""):
                out[k] = c.get(k)
        # Carry per-client minimum-hours threshold if set
        mh = c.get("min_outage_hours")
        if mh:
            try:
                out["min_outage_hours"] = float(mh)
            except (ValueError, TypeError):
                pass
        return out

    def opportunity_summary(definite, possible):
        """Return (longest_hours, total_hours) for non-cancelled outages only."""
        active = [o for o in (definite + possible) if o.get("status", "").lower() != "cancelled"]
        if not active:
            return 0.0, 0.0
        durations = [float(o.get("duration_hours") or 0) for o in active]
        return max(durations) if durations else 0.0, sum(durations)

    affected_payload = []
    for a in affected:
        longest, total = opportunity_summary(a["definite"], a["possible"])
        affected_payload.append({
            "client": slim_client(a["client"]),
            "definite": a["definite"],
            "possible": a["possible"],
            "nearest_street": a["nearest_street"],
            "nearest_distance_m": a["nearest_distance_m"],
            "longest_hours": round(longest, 2),
            "total_hours": round(total, 2),
        })

    # All clients for "show all clients" toggle on the map
    clients_payload = [slim_client(c) for c in clients]

    main_payload = {
        "suburbs": suburbs,
        "outages": [o for outs in outages_by_pair.values() for o in outs],
        "streets": streets,
        "matchedPairs": [f"{sub}||{st}" for sub, st in matched_pairs],
        "clients": clients_payload,
        "affected": affected_payload,
        "meta": {
            "source": "Jemena planned outages (jemena.com.au) · auto-updated",
            "extracted": now,
            "totalOutages": len(raw_outages),
            "totalClients": len(clients),
            "definiteCount": n_def,
            "possibleCount": n_pos,
            "bufferMetres": BUFFER_METRES,
            "defaultMinHours": DEFAULT_MIN_HOURS,
        },
    }

    affected_payload_full = {
        "affected": affected_payload,
        "meta": main_payload["meta"],
    }

    (DOCS / "index.html").write_text(render_main_html(main_payload), encoding="utf-8")
    (DOCS / "affected.html").write_text(render_affected_html(affected_payload_full), encoding="utf-8")
    (DOCS / "data.json").write_text(json.dumps(main_payload, separators=(",", ":")), encoding="utf-8")
    write_affected_csv(affected, DOCS / "affected.csv", DEFAULT_MIN_HOURS)

    size = (DOCS / "index.html").stat().st_size
    print(f"[done] wrote docs/index.html ({size:,}B), affected.html, affected.csv", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
