"""
Ausnet planned outage scraper.

Scrapes the outage list from outagetracker.com.au and emits records in the
same shape as Jemena outages so they can be matched together. The Next.js
build hash in URLs is resolved dynamically from the page's HTML so we don't
break when Ausnet redeploys.

Key concepts:
- Source list endpoint: ".../api/combinedoutage" (or Next.js data variant)
  returns ALL incidents with metadata (lat/lng centroid, customers, status, type).
- Per-incident endpoint: ".../api/<incident-id>" returns polygon coordinates.

We filter early to avoid pulling polygons we'll discard:
- Planned only (type: 'Planned'), not Cancelled
- Within MAX_DISTANCE_KM of the operator base
- Active (start/end in the future or now)
- Customer count >= MIN_CUSTOMERS (1 by default, see the note on the constant)
- Duration >= MIN_HOURS

Output format mirrors Jemena's `raw_outages`:
    {
        suburb: <derived best-guess>,
        street: <not available - polygon-based, no streets>,
        start_dt, end_dt: aware datetimes,
        start_display, end_display: formatted,
        status: 'Scheduled' or 'Cancelled',
        network: 'Ausnet',
        polygon: [[lat, lng], ...],
        polygon_centroid: (lat, lng),
        customers: int,
        incident_id: 'INCD-...',
    }
"""
from __future__ import annotations

import math
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Iterable

import requests


AUSNET_BASE = "https://www.outagetracker.com.au"
AUSNET_LIST_PAGE = f"{AUSNET_BASE}/outage-list"
AUSNET_HOME_PAGE = AUSNET_BASE
# The actual API lives on a separate host (discovered via Claude in Chrome).
# Configurable via env var in case the structure changes.
import os as _os
AUSNET_API_BASE = _os.environ.get(
    "AUSNET_API_BASE",
    "https://outagetrackerservice.ausnetservices.com.au"
).rstrip("/")

# Operator base (Carrum Downs, VIC)
OPERATOR_BASE_LAT = -38.0833
OPERATOR_BASE_LNG = 145.1833
MAX_DISTANCE_KM = 200.0

# Filters
# MIN_CUSTOMERS was 10 until Nov 2026. That dropped every outage affecting 1-9
# customers, which is exactly what a single supermarket, factory or servo on
# its own transformer looks like. Matching is against watched sites only, so a
# small outage costs nothing unless one of our sites is in it.
MIN_CUSTOMERS = int(_os.environ.get("AUSNET_MIN_CUSTOMERS", "1"))
MIN_HOURS = 6.0

# A boundary wider than this is not a real de-energised area: AusNet's hull
# occasionally swallows one mis-located meter (Nov 2026: a Seaspray outage
# drawn as a 193 km sliver to Ringwood, which put 283 unaffected sites "in
# zone"). Such polygons are clipped for display and never trusted on their own.
DEGENERATE_SPAN_KM = 40.0
REPAIR_RADIUS_KM = 25.0
# Outages with no published boundary (mostly 1-2 customer jobs) get a small
# search circle around AusNet's incident point. A site in it only counts when
# AusNet's own street search confirms the street.
SYNTHETIC_RADIUS_M = 250.0

AUSNET_SEARCH_PATH = "/api/v1/outages/search"
BOUNDARY_CACHE_FILE = "ausnet_boundaries.json"
STREET_SEARCH_CACHE_FILE = "ausnet_street_search.json"
STREET_SEARCH_TTL_HOURS = 3.0

# Network identity attached to each outage
NETWORK = "Ausnet"

USER_AGENT = "jemena-outage-map/6.0 (+https://github.com/Nexus-Energy393/jemena-outages)"
MELBOURNE_TZ = timezone(timedelta(hours=10))


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _haversine_km(lat1, lng1, lat2, lng2) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _parse_aus_dt(s: str) -> datetime | None:
    """Parse Ausnet's date strings into aware datetimes.

    Examples seen:
        '2025-07-22 09:50:03.703985 Australia/Melbourne'
        '2025-07-22 09:50AM'
        '' (empty)
    """
    if not s:
        return None
    s = s.strip()
    # Strip the trailing timezone label if present
    s = re.sub(r"\s+Australia/Melbourne$", "", s)
    s = re.sub(r"\s+Australia/[A-Za-z_]+$", "", s)

    formats = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %I:%M%p",
        "%Y-%m-%d %I:%M %p",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=MELBOURNE_TZ)
        except ValueError:
            continue
    return None


def _format_display_start(dt: datetime) -> str:
    return dt.strftime("%a %d %b, %I:%M %p").replace(" 0", " ")


def _format_display_end(dt: datetime) -> str:
    return dt.strftime("%I:%M %p").lstrip("0")


# ---------------------------------------------------------------------------
# Fetching with build-hash discovery
# ---------------------------------------------------------------------------
class AusnetClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
        self._build_id = None

    def _discover_build_id(self) -> str | None:
        """Pull the Next.js build ID from the home page HTML.

        Looks for: <script id="__NEXT_DATA__" ...> { "buildId": "..." } </script>
        """
        if self._build_id:
            return self._build_id
        try:
            r = self.session.get(AUSNET_HOME_PAGE, timeout=30)
            r.raise_for_status()
            html = r.text
            m = re.search(r'"buildId"\s*:\s*"([^"]+)"', html)
            if m:
                self._build_id = m.group(1)
                print(f"[ausnet] discovered buildId: {self._build_id}", flush=True)
                return self._build_id
        except Exception as e:
            print(f"[ausnet] could not discover buildId: {e}", flush=True)
        return None

    def _try_endpoints(self, paths: list[str]) -> dict | None:
        """Try a list of candidate URLs in order; return first JSON success."""
        for url in paths:
            try:
                r = self.session.get(url, timeout=60)
                if r.ok:
                    j = r.json()
                    return j
            except Exception:
                continue
        return None

    def fetch_list(self) -> list[dict]:
        """Fetch the combined-outage list. Returns a list of raw outage dicts."""
        # The real API lives on outagetrackerservice.ausnetservices.com.au
        candidates = [
            f"{AUSNET_API_BASE}/api/v1/outages/combinedoutage",
            f"{AUSNET_API_BASE}/api/combinedoutage",  # fallback
        ]
        # Also try Next.js data fallbacks (in case the API host changes someday)
        build_id = self._discover_build_id()
        if build_id:
            candidates += [
                f"{AUSNET_BASE}/_next/data/{build_id}/en.json",
            ]

        print(f"[ausnet] fetching outage list ({len(candidates)} candidate endpoints)", flush=True)
        result = self._try_endpoints(candidates)
        if not result:
            raise RuntimeError("Could not fetch Ausnet outage list from any endpoint")

        outages = self._extract_outage_list(result)
        if outages is None:
            raise RuntimeError("Got Ausnet response but could not find the outage array")
        print(f"[ausnet] {len(outages)} outages in raw list", flush=True)
        return outages

    def _extract_outage_list(self, payload: dict) -> list | None:
        """Drill into the response to find the outage array."""
        # Direct: { data: [...] }
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            return payload["data"]
        # Next.js: { pageProps: { ... } }
        page_props = (payload or {}).get("pageProps") or {}
        for key in ("outages", "data", "incidents", "combinedOutages",
                    "items", "combinedoutage", "list", "rows", "results"):
            v = page_props.get(key)
            if isinstance(v, list):
                return v
        # Search recursively as a last resort
        result = _find_first_list_of_dicts_with_field(payload, "incident")
        if result:
            return result
        # Try other plausible field names
        for field in ("id", "incidentId", "incidentNumber", "outageId"):
            result = _find_first_list_of_dicts_with_field(payload, field)
            if result:
                return result
        # Debug: log what we DID find
        print("[ausnet] could not find outage array. Top-level keys: "
              f"{list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__}",
              flush=True)
        if isinstance(payload, dict):
            for k, v in payload.items():
                if isinstance(v, dict):
                    print(f"[ausnet]   {k}: dict with keys {list(v.keys())[:20]}", flush=True)
                elif isinstance(v, list):
                    sample = v[0] if v else None
                    sample_keys = list(sample.keys())[:10] if isinstance(sample, dict) else type(sample).__name__
                    print(f"[ausnet]   {k}: list of {len(v)} (sample keys: {sample_keys})", flush=True)
                else:
                    print(f"[ausnet]   {k}: {type(v).__name__}", flush=True)
        return None

    def fetch_polygon(self, incident_id: str) -> list[list[float]] | None:
        """Fetch the polygon points for one incident.

        Returns [[lat, lng], ...] or None if not found.
        """
        # Confirmed endpoint (it is the one outagetracker.com.au itself calls,
        # read from the site's outageTrackerSettings). Incidents with no
        # boundary answer HTTP 500, so guessing other paths only tripled the
        # failed requests.
        candidates = [
            f"{AUSNET_API_BASE}/api/v1/outages/outageboundary/{incident_id}",
        ]
        result = self._try_endpoints(candidates)
        if not result:
            return None
        # Shape: { data: [{latitude, longitude}, ...], success: true }
        items = None
        if isinstance(result.get("data"), list):
            items = result["data"]
        else:
            page_props = (result or {}).get("pageProps") or {}
            for key in ("polygon", "data", "boundary", "coordinates"):
                v = page_props.get(key)
                if isinstance(v, list):
                    items = v
                    break
        if not items:
            return None
        coords = []
        for p in items:
            if isinstance(p, dict) and "latitude" in p and "longitude" in p:
                try:
                    coords.append([float(p["latitude"]), float(p["longitude"])])
                except (TypeError, ValueError):
                    continue
            elif isinstance(p, (list, tuple)) and len(p) >= 2:
                try:
                    coords.append([float(p[0]), float(p[1])])
                except (TypeError, ValueError):
                    continue
        return coords if len(coords) >= 3 else None


def _find_first_list_of_dicts_with_field(obj, field: str):
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict) and field in obj[0]:
            return obj
        for item in obj:
            r = _find_first_list_of_dicts_with_field(item, field)
            if r is not None:
                return r
    elif isinstance(obj, dict):
        for v in obj.values():
            r = _find_first_list_of_dicts_with_field(v, field)
            if r is not None:
                return r
    return None


# ---------------------------------------------------------------------------
# Filtering and normalisation
# ---------------------------------------------------------------------------
def _row_planned_start_end(row: dict) -> tuple[datetime | None, datetime | None]:
    """Build aware datetimes from whatever fields Ausnet provides.

    plannedDate, plannedStartTime, plannedEndTime are sometimes split:
        plannedDate: '2025-07-22' or '2025-07-22T00:00:00'
        plannedStartTime: '09:50AM'
        plannedEndTime: '04:30PM'
    Or sometimes given as full timestamps.
    """
    # Try the easy case first - full timestamps
    candidates_start = [
        row.get("plannedStartTime"),
        row.get("initialEstimatedTimeToRestoration"),
    ]
    candidates_end = [
        row.get("plannedEndTime"),
        row.get("latestEstimatedTimeToRestoration"),
        row.get("initialEstimatedTimeToRestoration"),
    ]

    start_dt = None
    for s in candidates_start:
        if s and len(s) > 10:  # Only try if it looks like a full timestamp
            start_dt = _parse_aus_dt(s)
            if start_dt:
                break

    end_dt = None
    for s in candidates_end:
        if s and len(s) > 10:
            end_dt = _parse_aus_dt(s)
            if end_dt:
                break

    # If we have plannedDate + a time string, combine them
    planned_date = row.get("plannedDate") or ""
    if not start_dt and planned_date and row.get("plannedStartTime"):
        start_dt = _combine_date_and_time(planned_date, row["plannedStartTime"])
    if not end_dt and planned_date and row.get("plannedEndTime"):
        end_dt = _combine_date_and_time(planned_date, row["plannedEndTime"])

    # Sanity: if end is before start (crosses midnight), bump end by 1 day
    if start_dt and end_dt and end_dt <= start_dt:
        end_dt = end_dt + timedelta(days=1)

    return start_dt, end_dt


def _combine_date_and_time(date_str: str, time_str: str) -> datetime | None:
    """e.g. '2025-07-22' + '09:50AM' -> aware datetime"""
    if not date_str or not time_str:
        return None
    # Strip 'T...' suffix from date if present
    date_str = date_str.split("T")[0].strip()
    time_str = time_str.strip()
    for time_fmt in ["%I:%M%p", "%I:%M %p", "%H:%M", "%H:%M:%S"]:
        try:
            t = datetime.strptime(time_str, time_fmt).time()
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
            return datetime.combine(d, t).replace(tzinfo=MELBOURNE_TZ)
        except ValueError:
            continue
    return None


def filter_and_normalise(
    raw_list: list[dict],
    *,
    min_customers: int = MIN_CUSTOMERS,
    min_hours: float = MIN_HOURS,
    max_distance_km: float = MAX_DISTANCE_KM,
    base_lat: float = OPERATOR_BASE_LAT,
    base_lng: float = OPERATOR_BASE_LNG,
) -> list[dict]:
    """Apply all the cheap filters to the raw list before fetching polygons.

    Returns a list of trimmed-down dicts ready for polygon fetching.
    """
    now = datetime.now(MELBOURNE_TZ)
    far_future = now + timedelta(days=365)

    kept = []
    rejection_counts = {
        "non_planned": 0,
        "cancelled": 0,
        "too_far": 0,
        "too_few_customers": 0,
        "no_dates": 0,
        "in_past": 0,
        "too_short": 0,
    }

    for row in raw_list:
        if (row.get("type") or "").strip().lower() != "planned":
            rejection_counts["non_planned"] += 1
            continue
        status_raw = (row.get("incidentStatus") or row.get("status") or "").strip().upper()
        if status_raw == "CANCELLED":
            rejection_counts["cancelled"] += 1
            # We still want to know cancelled outages exist (for Pipedrive cancellation handling),
            # but only if we'd previously care about them. Skip for now; expand later if needed.
            continue

        try:
            lat = float(row["latitude"])
            lng = float(row["longitude"])
        except (KeyError, TypeError, ValueError):
            continue

        d_km = _haversine_km(base_lat, base_lng, lat, lng)
        if d_km > max_distance_km:
            rejection_counts["too_far"] += 1
            continue

        try:
            customers = int(row.get("nmiCount") or 0)
        except (TypeError, ValueError):
            customers = 0
        if customers < min_customers:
            rejection_counts["too_few_customers"] += 1
            continue

        start_dt, end_dt = _row_planned_start_end(row)
        if not start_dt or not end_dt:
            rejection_counts["no_dates"] += 1
            continue
        if end_dt < now - timedelta(days=1):
            rejection_counts["in_past"] += 1
            continue
        if end_dt > far_future:
            continue
        duration_h = (end_dt - start_dt).total_seconds() / 3600.0
        if duration_h < min_hours:
            rejection_counts["too_short"] += 1
            continue

        incident_id = row.get("incident") or row.get("id") or ""
        if not incident_id:
            continue

        kept.append({
            "incident_id": incident_id,
            "centroid_lat": lat,
            "centroid_lng": lng,
            "towns": _row_towns(row),
            "last_updated": str(row.get("incidentLastUpdated") or row.get("statusLastUpdated") or ""),
            "category_id": row.get("categoryId") or "",
            "customers": customers,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "duration_hours": round(duration_h, 2),
            "status": "Scheduled",  # cancelled handled above
        })

    print(f"[ausnet] filtering: kept {len(kept)} of {len(raw_list)}; "
          f"rejections: {rejection_counts}", flush=True)
    return kept


# ---------------------------------------------------------------------------
# Towns, boundary hygiene, caches and AusNet's own street search
# ---------------------------------------------------------------------------
import json as _json
from pathlib import Path as _Path

_CACHE_DIR = _Path(__file__).parent / ".cache"


def norm_town(name) -> str:
    """'Mt. Eliza ' -> 'MOUNT ELIZA'. One spelling for our suburbs and theirs."""
    s = re.sub(r"[^A-Za-z0-9 ]+", " ", str(name or "")).upper()
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^MT ", "MOUNT ", s)
    return s


def _row_towns(row: dict) -> list[dict]:
    """details[] -> [{name, postcode, customers}], biggest first. AusNet lists
    every locality with at least one affected meter, which makes it the
    authority on WHERE an outage is (the boundary alone is not, see
    DEGENERATE_SPAN_KM)."""
    out = []
    for d in row.get("details") or []:
        if not isinstance(d, dict):
            continue
        name = norm_town(d.get("townName"))
        if not name:
            continue
        pc = re.sub(r"\D", "", str(d.get("postCode") or ""))
        try:
            cc = int(d.get("customerCount") or 0)
        except (TypeError, ValueError):
            cc = 0
        out.append({"name": name, "postcode": pc, "customers": cc})
    out.sort(key=lambda t: -t["customers"])
    return out


def town_label(towns: list[dict], limit: int = 3) -> str:
    names = [t["name"].title() for t in towns[:limit]]
    extra = len(towns) - len(names)
    return ", ".join(names) + (f" +{extra} more" if extra > 0 else "")


def _span_km(polygon) -> float:
    lats = [p[0] for p in polygon]
    lngs = [p[1] for p in polygon]
    return _haversine_km(min(lats), min(lngs), max(lats), max(lngs))


def repair_polygon(polygon, anchor_lat, anchor_lng):
    """Returns (display_polygon, degenerate). A sane boundary comes back
    untouched. A degenerate one is clipped to the vertices near AusNet's own
    incident point so the map shows the real area instead of a sliver across
    the state; if too little survives, a circle stands in."""
    if _span_km(polygon) <= DEGENERATE_SPAN_KM:
        return polygon, False
    near = [p for p in polygon if _haversine_km(anchor_lat, anchor_lng, p[0], p[1]) <= REPAIR_RADIUS_KM]
    if len(near) >= 3:
        return near, True
    return circle_polygon(anchor_lat, anchor_lng, 2000.0), True


def circle_polygon(lat, lng, radius_m, points: int = 16):
    out = []
    for i in range(points):
        a = 2 * math.pi * i / points
        out.append([
            lat + (radius_m * math.sin(a)) / 110540.0,
            lng + (radius_m * math.cos(a)) / (111320.0 * math.cos(math.radians(lat))),
        ])
    return out


def _load_json_cache(name: str) -> dict:
    f = _CACHE_DIR / name
    try:
        return _json.loads(f.read_text()) if f.exists() else {}
    except Exception:
        return {}


def _save_json_cache(name: str, data: dict) -> None:
    try:
        _CACHE_DIR.mkdir(exist_ok=True)
        (_CACHE_DIR / name).write_text(_json.dumps(data, sort_keys=True))
    except Exception as e:  # noqa: BLE001 - a cache is never worth a failed run
        print(f"[ausnet] could not save {name}: {e}", flush=True)


def clean_street(address: str) -> str:
    """'94/72-82 Princes Drive, Morwell' -> 'Princes Drive'. Returns '' unless
    the address is a plain numbered street address, because the answer feeds a
    yes/no question to AusNet and a garbled street reads as a false 'no'."""
    s = (address or "").strip()
    if not s or re.search(r"\b(cnr|corner)\b|&| and ", s, re.IGNORECASE):
        return ""
    s = re.sub(r"^(shop|unit|suite|level|lot|tenancy|kiosk)\s*[\w-]+\s*[,/]?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^[\w-]{1,6}\s*/\s*(?=\d)", "", s)  # "94/72-82 ..." -> "72-82 ..."
    m = re.match(r"^\d+[A-Za-z]?(?:\s*[-–]\s*\d+[A-Za-z]?)?\s+([A-Za-z][A-Za-z'. -]*?)\s*(?:,|$)", s)
    if not m:
        return ""
    street = re.sub(r"\s+", " ", m.group(1)).strip(" .-")
    # One-word names ("Main") would prefix-match Main Road AND Main Street.
    return street if len(street.split()) >= 2 else ""


class StreetSearch:
    """AusNet's own "is this street in an outage" lookup: the POST that the
    outagetracker.com.au search box makes. It answers from AusNet's meter
    database, so it is the street-level truth the boundary polygon only
    approximates. Looked up once per street+suburb, a few at a time, and
    remembered for STREET_SEARCH_TTL_HOURS so hourly runs stay polite."""

    def __init__(self):
        self.cache = _load_json_cache(STREET_SEARCH_CACHE_FILE)
        self.calls = 0
        self.errors = 0

    @staticmethod
    def key(street: str, suburb: str) -> str:
        return f"{street.strip().upper()}|{suburb.strip().upper()}"

    def _fresh(self, k: str):
        hit = self.cache.get(k)
        if not hit:
            return None
        try:
            age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(hit["at"])).total_seconds() / 3600.0
        except Exception:
            return None
        return hit["incidents"] if age_h <= STREET_SEARCH_TTL_HOURS else None

    def _fetch(self, street: str, suburb: str):
        body = {"incidentId": "", "nmi": "", "streetName": street.strip(), "suburb": suburb.strip(), "postCode": ""}
        for attempt in (1, 2):
            try:
                r = requests.post(
                    AUSNET_API_BASE + AUSNET_SEARCH_PATH, json=body, timeout=30,
                    headers={"User-Agent": USER_AGENT, "Accept": "application/json",
                             "Origin": AUSNET_BASE, "Referer": AUSNET_BASE + "/"},
                )
                if r.status_code == 204:
                    return []
                r.raise_for_status()
                data = r.json() if r.content else []
                if not isinstance(data, list):
                    raise ValueError("unexpected search response shape")
                return sorted({str(x.get("incident") or x.get("id")) for x in data if isinstance(x, dict)})
            except Exception as e:  # noqa: BLE001
                if attempt == 2:
                    print(f"[ausnet-search] {street}, {suburb}: {e}", flush=True)
                time.sleep(1.5)
        return None

    def lookup_many(self, pairs) -> dict:
        """pairs: iterable of (street, suburb). Returns {key: [incident ids]}
        with None for a lookup that failed (unknown, never a 'no')."""
        from concurrent.futures import ThreadPoolExecutor
        todo, out = [], {}
        for street, suburb in {(s, b) for s, b in pairs if s and b}:
            k = self.key(street, suburb)
            cached = self._fresh(k)
            if cached is not None:
                out[k] = cached
            else:
                todo.append((k, street, suburb))
        if todo:
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(lambda t: (t[0], self._fetch(t[1], t[2])), todo))
            now = datetime.now(timezone.utc).isoformat()
            for k, incidents in results:
                self.calls += 1
                out[k] = incidents
                if incidents is None:
                    self.errors += 1
                else:
                    self.cache[k] = {"incidents": incidents, "at": now}
            # Keep the file small: drop entries past their use-by.
            self.cache = {k: v for k, v in self.cache.items() if self._fresh(k) is not None}
            _save_json_cache(STREET_SEARCH_CACHE_FILE, self.cache)
        return out


# ---------------------------------------------------------------------------
# Top-level entry — called from scrape.py
# ---------------------------------------------------------------------------
def scrape_ausnet(
    min_customers: int = MIN_CUSTOMERS,
    min_hours: float = MIN_HOURS,
    max_distance_km: float = MAX_DISTANCE_KM,
) -> list[dict]:
    """Returns a list of normalised Ausnet outages with polygons attached.

    Output dicts have:
        suburb          the localities AusNet lists, biggest first ("Cowes")
        towns           [{name, postcode, customers}] straight from details[]
        street          always '' (polygon-based)
        start_dt, end_dt, start_display, end_display, status, duration_hours
        network: 'Ausnet'
        polygon         the boundary used to FIND candidate sites (as published,
                        or a small circle when AusNet publishes none)
        polygon_display the boundary to draw (clipped when degenerate)
        polygon_degenerate / polygon_synthetic   trust flags for verify.py
        polygon_centroid: (lat,lng)
        customers, incident_id
    """
    client = AusnetClient()

    raw = client.fetch_list()
    candidates = filter_and_normalise(
        raw,
        min_customers=min_customers,
        min_hours=min_hours,
        max_distance_km=max_distance_km,
    )
    if not candidates:
        print("[ausnet] no candidates passed the cheap filters; nothing to fetch", flush=True)
        return []

    # Boundaries only change when AusNet touches the incident, so remember them
    # against the incident's last-updated stamp instead of re-fetching ~200
    # every run. "p": None records "AusNet publishes no boundary for this one".
    cache = _load_json_cache(BOUNDARY_CACHE_FILE)
    fresh_cache = {}
    out = []
    counts = {"published": 0, "synthetic": 0, "degenerate": 0}
    polygons = {}
    need = []
    for c in candidates:
        hit = cache.get(c["incident_id"])
        if hit and hit.get("u") == c["last_updated"] and "p" in hit:
            polygons[c["incident_id"]] = hit["p"]
        else:
            need.append(c["incident_id"])
    reused, fetched = len(polygons), len(need)
    if need:
        # A few at a time: the first run after a cold cache has ~230 to fetch.
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=4) as pool:
            for iid, poly in zip(need, pool.map(client.fetch_polygon, need)):
                polygons[iid] = poly
    for c in candidates:
        iid = c["incident_id"]
        polygon = polygons.get(iid)
        fresh_cache[iid] = {"u": c["last_updated"], "p": polygon}

        synthetic = False
        if not polygon:
            polygon = circle_polygon(c["centroid_lat"], c["centroid_lng"], SYNTHETIC_RADIUS_M)
            synthetic = True
        display, degenerate = repair_polygon(polygon, c["centroid_lat"], c["centroid_lng"])
        counts["synthetic" if synthetic else "published"] += 1
        if degenerate:
            counts["degenerate"] += 1
            print(f"[ausnet] {iid}: boundary spans {_span_km(polygon):.0f} km for {c['customers']} customers "
                  f"({town_label(c['towns'])}); clipped for display, matches need town + street proof", flush=True)

        plat = sum(p[0] for p in display) / len(display)
        plng = sum(p[1] for p in display) / len(display)
        out.append({
            "suburb": town_label(c["towns"]),
            "towns": c["towns"],
            "street": "",  # polygon-based, no street list
            "street_raw": "",
            "start_dt": c["start_dt"],
            "end_dt": c["end_dt"],
            "start_display": _format_display_start(c["start_dt"]),
            "end_display": _format_display_end(c["end_dt"]),
            "status": c["status"],
            "duration_hours": c["duration_hours"],
            "network": NETWORK,
            "polygon": polygon,
            "polygon_display": display,
            "polygon_degenerate": degenerate,
            "polygon_synthetic": synthetic,
            "polygon_centroid": (plat, plng),
            "anchor": (c["centroid_lat"], c["centroid_lng"]),
            "customers": c["customers"],
            "incident_id": iid,
        })
    _save_json_cache(BOUNDARY_CACHE_FILE, fresh_cache)

    print(f"[ausnet] done: {len(out)} outages ({counts['published']} with a published boundary, "
          f"{counts['synthetic']} point-only, {counts['degenerate']} degenerate); "
          f"boundaries fetched {fetched}, reused from cache {reused}", flush=True)
    return out


# ---------------------------------------------------------------------------
# Polygon containment + buffered match (for client matching in scrape.py)
# ---------------------------------------------------------------------------
def point_in_polygon(plat: float, plng: float, polygon: list[list[float]]) -> bool:
    """Standard ray-casting test. Polygon coords are [lat, lng] pairs."""
    n = len(polygon)
    if n < 3:
        return False
    inside = False
    j = n - 1
    for i in range(n):
        yi, xi = polygon[i][0], polygon[i][1]
        yj, xj = polygon[j][0], polygon[j][1]
        intersect = ((yi > plat) != (yj > plat)) and \
                    (plng < (xj - xi) * (plat - yi) / ((yj - yi) or 1e-9) + xi)
        if intersect:
            inside = not inside
        j = i
    return inside


def polygon_distance_m(plat: float, plng: float, polygon: list[list[float]]) -> float:
    """Approximate min distance in metres from point to polygon edges.

    If the point is inside the polygon, returns 0.
    """
    if point_in_polygon(plat, plng, polygon):
        return 0.0
    # Distance to each edge
    best = math.inf
    n = len(polygon)
    for i in range(n):
        a = polygon[i]
        b = polygon[(i + 1) % n]
        d = _point_to_segment_m(plat, plng, a[0], a[1], b[0], b[1])
        if d < best:
            best = d
    return best


def _point_to_segment_m(plat, plng, alat, alng, blat, blng):
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


def polygon_bbox(polygon: list[list[float]]) -> tuple[float, float, float, float]:
    """Returns (minlat, minlng, maxlat, maxlng)."""
    lats = [p[0] for p in polygon]
    lngs = [p[1] for p in polygon]
    return min(lats), min(lngs), max(lats), max(lngs)
