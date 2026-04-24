"""
Scrape Jemena planned outages, match streets to OpenStreetMap geometry,
and rebuild docs/index.html.

The Jemena page is a <table> where each suburb is a collapsible header row
(class="cursor-pointer"), with detail rows hidden until clicked. So we click
every suburb header before extracting.

Usage:
    pip install -r requirements.txt
    python -m playwright install chromium
    python scrape.py

Debug output (always written, even on failure):
    docs/_last_scrape.png      - full-page screenshot after expansion
    docs/_last_scrape.html     - rendered HTML after expansion
    docs/_last_scrape_raw.json - raw table rows as extracted
"""
from __future__ import annotations

import asyncio
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
USER_AGENT = f"jemena-outage-map/1.1 (+{REPO_URL})"
MELBOURNE_TZ = timezone(timedelta(hours=10))


# ---------------------------------------------------------------------------
# Street name normalisation
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
    name = " ".join(out)
    return "-".join(w[:1].upper() + w[1:] for w in name.split("-"))


def norm_key(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


# ---------------------------------------------------------------------------
# Date / time parsing
# ---------------------------------------------------------------------------
MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}


def parse_date_str(s: str) -> datetime | None:
    """Parse '06-May' into an aware datetime (midnight, Melbourne).

    Year is inferred: pick the year that puts the date within ~6 months of
    today. Planned outages are always future-dated on the page, so if today
    is 24-Apr-2026 and we see '06-Jan', that's Jan-2027.
    """
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


def parse_time_range(s: str) -> tuple[tuple[int, int] | None, tuple[int, int] | None]:
    """Parse '0730-1730' or '0730 - 1730' into ((7,30), (17,30))."""
    m = re.match(r"(\d{3,4})\s*[-–]\s*(\d{3,4})", s.strip())
    if not m:
        return None, None

    def hm(chunk: str):
        chunk = chunk.zfill(4)
        return int(chunk[:2]), int(chunk[2:])

    return hm(m.group(1)), hm(m.group(2))


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------
async def scrape_outages() -> list[dict]:
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

            # Dismiss cookie banners
            for sel in [
                'button:has-text("Accept")',
                'button:has-text("Accept all")',
                'button:has-text("Allow")',
                '[aria-label*="accept" i]',
            ]:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=500):
                        await btn.click()
                        await page.wait_for_timeout(500)
                        break
                except Exception:
                    pass

            # Wait for the outage table to exist
            try:
                await page.wait_for_selector("table tbody tr", timeout=30000)
            except Exception:
                pass

            # Click every collapsible suburb header to reveal detail rows.
            # We click by JS rather than Playwright's .click() to avoid
            # flakiness from overlapping elements.
            clicked = await page.evaluate(
                """() => {
                    const headers = document.querySelectorAll('tr.cursor-pointer');
                    headers.forEach(h => h.click());
                    return headers.length;
                }"""
            )
            print(f"[scrape] clicked {clicked} suburb headers", flush=True)
            await page.wait_for_timeout(2000)

            # Save debug snapshots after expansion, always
            try:
                await page.screenshot(
                    path=str(DOCS / "_last_scrape.png"), full_page=True, timeout=30000
                )
            except Exception as e:
                print(f"[debug] screenshot failed: {e}", flush=True)
            try:
                (DOCS / "_last_scrape.html").write_text(
                    await page.content(), encoding="utf-8"
                )
            except Exception as e:
                print(f"[debug] content save failed: {e}", flush=True)

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
                        // Score by number of 6-cell rows (real data rows)
                        const dataRows = cells.filter(r => r.length >= 6).length;
                        if (dataRows > bestCount) { bestCount = dataRows; bestRows = cells; }
                    }
                    return bestRows || [];
                }"""
            )

            # Save the raw extraction for debugging
            try:
                (DOCS / "_last_scrape_raw.json").write_text(
                    json.dumps(rows, indent=2), encoding="utf-8"
                )
            except Exception:
                pass

        finally:
            await browser.close()

    if not rows:
        raise RuntimeError("Could not find any table on the Jemena page.")

    outages = _parse_table(rows)
    if not outages:
        raise RuntimeError(
            f"Found {len(rows)} table rows but none looked like outage data. "
            f"Check docs/_last_scrape_raw.json."
        )
    return outages


def _parse_table(rows: list[list[dict]]) -> list[dict]:
    """Expect columns: Suburb | Street | Date (dd-Mon) | Day | Time (HHMM-HHMM) | Status.

    Skip single-cell rows (suburb-header summary rows with colspan=6) and
    any row whose first cell doesn't look like a suburb (all-caps name).
    """
    out = []
    for row in rows:
        if len(row) < 6:
            continue  # header or single-cell suburb row

        cells = [c["text"] for c in row[:6]]
        suburb_text, street, date_str, day_str, time_str, status = cells

        # The suburb column should be all-caps-ish text, not a header
        if not suburb_text or suburb_text.strip().lower() in {"suburb", "location"}:
            continue

        # Parse date and time
        date_dt = parse_date_str(date_str)
        if date_dt is None:
            continue
        start_hm, end_hm = parse_time_range(time_str)
        if start_hm is None or end_hm is None:
            continue

        start_dt = date_dt.replace(hour=start_hm[0], minute=start_hm[1])
        end_dt = date_dt.replace(hour=end_hm[0], minute=end_hm[1])
        if end_dt <= start_dt:
            # Outage crosses midnight
            end_dt = end_dt.replace(day=end_dt.day + 1) if end_dt.day < 28 else end_dt

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
# Geocoding
# ---------------------------------------------------------------------------
GEO_CACHE_FILE = CACHE / "suburbs.json"


def _load_geo_cache() -> dict:
    if GEO_CACHE_FILE.exists():
        try:
            return json.loads(GEO_CACHE_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_geo_cache(cache: dict) -> None:
    GEO_CACHE_FILE.write_text(json.dumps(cache, indent=2, sort_keys=True))


def geocode_suburb(name: str) -> dict | None:
    cache = _load_geo_cache()
    key = name.upper()
    if key in cache:
        return cache[key]

    print(f"[geocode] {name}", flush=True)
    try:
        r = requests.get(
            NOMINATIM_URL,
            params={
                "q": f"{name}, Victoria, Australia",
                "format": "json",
                "limit": 1,
            },
            headers={"User-Agent": USER_AGENT, "Accept-Language": "en-AU"},
            timeout=30,
        )
        r.raise_for_status()
        results = r.json()
    except Exception as e:
        print(f"[geocode] failed for {name}: {e}", flush=True)
        return None
    time.sleep(1.2)  # respect Nominatim 1 req/sec policy

    if not results:
        return None
    loc = {"lat": float(results[0]["lat"]), "lng": float(results[0]["lon"])}
    cache[key] = loc
    _save_geo_cache(cache)
    return loc


# ---------------------------------------------------------------------------
# Overpass
# ---------------------------------------------------------------------------
def regex_escape_minimal(s: str) -> str:
    return re.sub(r"([.^$*+?()\[\]{}|\\])", r"\\\1", s)


def build_overpass_query(suburb_streets, suburb_geo) -> str:
    lines = ["[out:json][timeout:180];", "("]
    for suburb in sorted(suburb_streets):
        g = suburb_geo.get(suburb)
        if not g:
            continue
        streets = sorted(suburb_streets[suburb])
        names = "|".join(regex_escape_minimal(s) for s in streets)
        lines.append(
            f'  way["name"~"^({names})$",i]["highway"]'
            f'(around:3500,{g["lat"]},{g["lng"]});'
        )
    lines += [");", "out geom;"]
    return "\n".join(lines)


def fetch_overpass(query: str) -> dict:
    last_err = None
    for ep in OVERPASS_ENDPOINTS:
        try:
            print(f"[overpass] trying {ep}", flush=True)
            r = requests.post(
                ep,
                data={"data": query},
                headers={"User-Agent": USER_AGENT},
                timeout=300,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"[overpass] {ep} failed: {e}", flush=True)
            last_err = e
    raise RuntimeError(f"All Overpass endpoints failed; last: {last_err}")


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------
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
# Rendering
# ---------------------------------------------------------------------------
def render_html(data: dict) -> str:
    template = TEMPLATE.read_text(encoding="utf-8")
    return template.replace("__DATA__", json.dumps(data, separators=(",", ":")))


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

    # Group streets per suburb
    suburb_streets: dict[str, set] = {}
    for o in raw_outages:
        suburb_streets.setdefault(o["suburb"], set()).add(o["street"])
    suburb_streets = {k: sorted(v) for k, v in suburb_streets.items()}

    # Geocode
    suburb_geo = {}
    for sub in sorted(suburb_streets):
        loc = geocode_suburb(sub)
        if loc:
            suburb_geo[sub] = loc
        else:
            print(f"[warn] skipping {sub}: could not geocode", flush=True)

    if not suburb_geo:
        print("[fatal] no suburbs geocoded", flush=True)
        return 1

    # Overpass
    query = build_overpass_query(suburb_streets, suburb_geo)
    try:
        overpass = fetch_overpass(query)
    except Exception as e:
        print(f"[fatal] overpass failed: {e}", flush=True)
        return 1

    # Build indices
    outages_by_pair = {}
    for o in raw_outages:
        outages_by_pair.setdefault(
            (o["suburb"].lower(), norm_key(o["street"])), []
        ).append({
            "suburb": o["suburb"].title(),
            "street": o["street"],
            "start": o["start_display"],
            "end": o["end_display"],
            "status": o["status"],
        })

    pairs_by_street = {}
    for (suburb_l, street_n), outs in outages_by_pair.items():
        g = suburb_geo.get(suburb_l.upper())
        if g:
            pairs_by_street.setdefault(street_n, []).append(
                (suburb_l.title(), g["lat"], g["lng"])
            )

    streets = match_streets(overpass, outages_by_pair, pairs_by_street)
    print(f"[match] {len(streets)} street segments matched", flush=True)

    # Suburb marker summaries
    by_suburb = {}
    for o in raw_outages:
        b = by_suburb.setdefault(o["suburb"], {
            "name": o["suburb"].title(),
            "planned": 0,
            "cancelled": 0,
            "streets": set(),
        })
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
            "name": v["name"],
            "lat": g["lat"],
            "lng": g["lng"],
            "planned": v["planned"],
            "cancelled": v["cancelled"],
            "streets": sorted(v["streets"]),
        })

    matched_pairs = sorted({
        (s["suburb"].lower(), norm_key(s["name"])) for s in streets
    })

    now = datetime.now(MELBOURNE_TZ).strftime("%a %d %b %Y, %I:%M %p AEST")
    payload = {
        "suburbs": suburbs,
        "outages": [o for outs in outages_by_pair.values() for o in outs],
        "streets": streets,
        "matchedPairs": [f"{sub}||{st}" for sub, st in matched_pairs],
        "meta": {
            "source": "Jemena planned outages (jemena.com.au) · auto-updated",
            "extracted": now,
            "totalOutages": len(raw_outages),
        },
    }

    (DOCS / "index.html").write_text(render_html(payload), encoding="utf-8")
    (DOCS / "data.json").write_text(
        json.dumps(payload, separators=(",", ":")), encoding="utf-8"
    )
    size = (DOCS / "index.html").stat().st_size
    print(f"[done] wrote docs/index.html ({size:,} bytes)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
