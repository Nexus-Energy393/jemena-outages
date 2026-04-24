"""
Scrape Jemena planned outages, fetch street geometries from OpenStreetMap,
rebuild the interactive map at docs/index.html.

Designed to run in GitHub Actions once a day, but safe to run locally:

    pip install -r requirements.txt
    python -m playwright install chromium
    python scrape.py

Debug output (when scraping fails) goes in docs/_last_scrape.png and
docs/_last_scrape.html so you can see what the page looked like.
"""
from __future__ import annotations

import asyncio
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlencode

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

# Overpass mirrors, tried in order.
OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]

# Nominatim (suburb geocoding). Free, 1 req/sec rate limit.
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

# Identify ourselves politely. Replace the URL once you know your repo URL.
REPO_URL = os.environ.get("REPO_URL", "https://github.com/your-user/jemena-outages")
USER_AGENT = f"jemena-outage-map/1.0 (+{REPO_URL})"

MELBOURNE_TZ = timezone(timedelta(hours=10))  # ignores DST; good enough for display


# ---------------------------------------------------------------------------
# Street name normalisation (Jemena abbreviations → OSM full forms)
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
# Scraping: render Jemena page with Playwright and extract the outage table
# ---------------------------------------------------------------------------
async def scrape_outages() -> list[dict]:
    """Return a list of outage dicts. Raise RuntimeError if scrape fails."""
    from playwright.async_api import async_playwright  # imported late

    print(f"[scrape] loading {JEMENA_URL}", flush=True)
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1400, "height": 2200},
        )
        page = await ctx.new_page()

        await page.goto(JEMENA_URL, wait_until="domcontentloaded", timeout=90000)
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except Exception:
            pass
        await page.wait_for_timeout(4000)  # let client-side rendering settle

        # Try to click away cookie banners if present
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

        # Save debug snapshots on every run (small, overwritten each time)
        await page.screenshot(path=str(DOCS / "_last_scrape.png"), full_page=True)
        (DOCS / "_last_scrape.html").write_text(
            await page.content(), encoding="utf-8"
        )

        rows = await page.evaluate(
            """() => {
            const tables = [...document.querySelectorAll('table')];
            const candidates = tables
                .map(t => {
                    const trs = [...t.querySelectorAll('tr')];
                    const cells = trs.map(tr =>
                        [...tr.querySelectorAll('td,th')].map(c => (c.innerText || '').trim())
                    );
                    return { cells, rowCount: cells.length };
                })
                .filter(c => c.rowCount >= 3);
            if (!candidates.length) return { type: 'none', cells: [] };
            candidates.sort((a, b) => b.rowCount - a.rowCount);
            return { type: 'table', cells: candidates[0].cells };
        }"""
        )

        await browser.close()

    if rows.get("type") != "table" or not rows.get("cells"):
        raise RuntimeError(
            "Could not find an outage table on the Jemena page. "
            "Open docs/_last_scrape.png to see what loaded."
        )

    outages = _parse_table(rows["cells"])
    if not outages:
        raise RuntimeError(
            "Scraped table but got zero outage rows. Check docs/_last_scrape.html "
            "and update _parse_table() if the table structure changed."
        )
    return outages


def _parse_table(cells: list[list[str]]) -> list[dict]:
    """Interpret the scraped table rows."""
    if not cells:
        return []

    headers = [h.lower() for h in cells[0]]

    def col(*names):
        for i, h in enumerate(headers):
            if any(n in h for n in names):
                return i
        return None

    i_sub = col("suburb", "location")
    i_str = col("street")
    i_start = col("start", "from")
    i_end = col("end", "to", "finish")
    i_status = col("status", "note", "state")

    if i_sub is None or i_str is None:
        print(f"[parse] unexpected headers: {headers}", flush=True)
        # Fall back to positional: Suburb, Street, Start, End, Status (best guess)
        i_sub = i_sub if i_sub is not None else 0
        i_str = i_str if i_str is not None else 1
        i_start = i_start if i_start is not None else 2
        i_end = i_end if i_end is not None else 3
        i_status = i_status if i_status is not None else 4

    out = []
    for row in cells[1:]:
        if len(row) <= max(filter(lambda v: v is not None, [i_sub, i_str])):
            continue
        suburb = row[i_sub].strip()
        street = row[i_str].strip()
        if not suburb or not street:
            continue
        start = row[i_start].strip() if i_start is not None and i_start < len(row) else ""
        end = row[i_end].strip() if i_end is not None and i_end < len(row) else ""
        status = (row[i_status].strip() if i_status is not None and i_status < len(row) else "") or "Scheduled"
        out.append(
            {
                "suburb": suburb.upper(),
                "street_raw": street,
                "street": normalise_street(street),
                "start": start,
                "end": end,
                "status": status,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Geocoding: Nominatim, cached by suburb name
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
                "addressdetails": 0,
            },
            headers={"User-Agent": USER_AGENT, "Accept-Language": "en-AU"},
            timeout=30,
        )
        r.raise_for_status()
        results = r.json()
    except Exception as e:
        print(f"[geocode] failed for {name}: {e}", flush=True)
        return None
    time.sleep(1.2)  # Nominatim usage policy: ≤1 req/sec

    if not results:
        return None
    loc = {"lat": float(results[0]["lat"]), "lng": float(results[0]["lon"])}
    cache[key] = loc
    _save_geo_cache(cache)
    return loc


# ---------------------------------------------------------------------------
# Overpass: one query per run, fetches all affected street geometries
# ---------------------------------------------------------------------------
def regex_escape_minimal(s: str) -> str:
    return re.sub(r"([.^$*+?()\[\]{}|\\])", r"\\\1", s)


def build_overpass_query(suburb_streets: dict[str, list[str]], suburb_geo: dict[str, dict]) -> str:
    lines = ["[out:json][timeout:180];", "("]
    for suburb in sorted(suburb_streets):
        g = suburb_geo.get(suburb)
        if not g:
            continue
        streets = sorted(suburb_streets[suburb])
        names = "|".join(regex_escape_minimal(s) for s in streets)
        lines.append(
            f'  way["name"~"^({names})$",i]["highway"](around:3500,{g["lat"]},{g["lng"]});'
        )
    lines += [");", "out geom;"]
    return "\n".join(lines)


def fetch_overpass(query: str) -> dict:
    last_err: Exception | None = None
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
    raise RuntimeError(f"All Overpass endpoints failed; last error: {last_err}")


# ---------------------------------------------------------------------------
# Assembly: match OSM geometries back to Jemena (suburb, street) pairs
# ---------------------------------------------------------------------------
def match_streets(
    overpass: dict,
    suburb_geo: dict[str, dict],
    outages_by_pair: dict[tuple, list[dict]],
    pairs_by_street: dict[str, list[tuple]],
    max_dist_deg: float = 0.04,
) -> list[dict]:
    max_sq = max_dist_deg * max_dist_deg
    kept: list[dict] = []
    for el in overpass.get("elements", []):
        if el.get("type") != "way":
            continue
        geom = el.get("geometry") or []
        if len(geom) < 2:
            continue
        tags = el.get("tags") or {}
        name = tags.get("name")
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
        kept.append(
            {
                "name": name,
                "suburb": best,
                "coords": coords,
                "outages": outs,
                "cancelled": all(o["status"].lower() == "cancelled" for o in outs),
            }
        )
    return kept


# ---------------------------------------------------------------------------
# Write docs/index.html
# ---------------------------------------------------------------------------
def summarise_for_markers(outages: list[dict]) -> list[dict]:
    by_suburb: dict[str, dict] = {}
    for o in outages:
        s = by_suburb.setdefault(
            o["suburb"],
            {"name": o["suburb"].title(), "planned": 0, "cancelled": 0, "streets": set()},
        )
        s["streets"].add(o["street"])
        if o["status"].lower() == "cancelled":
            s["cancelled"] += 1
        else:
            s["planned"] += 1
    return [
        {
            "name": v["name"],
            "planned": v["planned"],
            "cancelled": v["cancelled"],
            "streets": sorted(v["streets"]),
        }
        for v in by_suburb.values()
    ]


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
        return 1

    print(f"[scrape] got {len(raw_outages)} outages", flush=True)

    # Deduplicate and group
    suburb_streets: dict[str, set] = {}
    for o in raw_outages:
        suburb_streets.setdefault(o["suburb"], set()).add(o["street"])
    suburb_streets = {k: sorted(v) for k, v in suburb_streets.items()}

    # Geocode each suburb
    suburb_geo: dict[str, dict] = {}
    for sub in sorted(suburb_streets):
        loc = geocode_suburb(sub)
        if loc:
            suburb_geo[sub] = loc
        else:
            print(f"[warn] skipping {sub}: could not geocode", flush=True)

    if not suburb_geo:
        print("[fatal] no suburbs could be geocoded", flush=True)
        return 1

    # Fetch Overpass
    query = build_overpass_query(suburb_streets, suburb_geo)
    try:
        overpass = fetch_overpass(query)
    except Exception as e:
        print(f"[fatal] overpass failed: {e}", flush=True)
        return 1

    # Build indices for matching
    outages_by_pair: dict[tuple, list[dict]] = {}
    for o in raw_outages:
        outages_by_pair.setdefault(
            (o["suburb"].lower(), norm_key(o["street"])), []
        ).append(
            {
                "suburb": o["suburb"].title(),
                "street": o["street"],
                "start": o["start"],
                "end": o["end"],
                "status": o["status"],
            }
        )

    pairs_by_street: dict[str, list[tuple]] = {}
    for (suburb_l, street_n), outs in outages_by_pair.items():
        g = suburb_geo.get(suburb_l.upper())
        if g:
            pairs_by_street.setdefault(street_n, []).append(
                (suburb_l.title(), g["lat"], g["lng"])
            )

    streets = match_streets(overpass, suburb_geo, outages_by_pair, pairs_by_street)
    print(f"[match] {len(streets)} street segments matched", flush=True)

    matched_pairs = sorted({(s["suburb"].lower(), norm_key(s["name"])) for s in streets})
    suburbs = summarise_for_markers(
        [o for outs in outages_by_pair.values() for o in outs]
    )
    # Attach coords
    for s in suburbs:
        g = suburb_geo.get(s["name"].upper())
        if g:
            s["lat"] = g["lat"]
            s["lng"] = g["lng"]
    suburbs = [s for s in suburbs if "lat" in s]

    now = datetime.now(MELBOURNE_TZ).strftime("%a %d %b %Y, %I:%M %p AEST")
    payload = {
        "suburbs": suburbs,
        "outages": [o for outs in outages_by_pair.values() for o in outs],
        "streets": streets,
        "matchedPairs": [f"{sub}||{st}" for sub, st in matched_pairs],
        "meta": {
            "source": f'Jemena planned outages (jemena.com.au) · auto-updated',
            "extracted": now,
            "totalOutages": len(raw_outages),
        },
    }

    (DOCS / "index.html").write_text(render_html(payload), encoding="utf-8")
    (DOCS / "data.json").write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    print(f"[done] wrote docs/index.html ({(DOCS / 'index.html').stat().st_size:,} bytes)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
