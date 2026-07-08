"""
Victoria Power Networks outage scrapers: CitiPower, Powercor and United Energy.

All three are Victoria Power Networks distributors and publish JSON outage
feeds with real de-energisation polygons (the same precise geometry Jemena
provides, and far tighter than Ausnet's loose outer rings):

  - CitiPower + Powercor share one feed ("cppc-outage"): a ROWSET.ROW[] list
    where each row carries BUSINESS, TOWN, POSTCODE, START_TIME, ETR, CAUSE,
    PRIVATISED (street), and a GeoJSON Polygon.
  - United Energy publishes "outages-v2.json": an {outages:[...]} list with
    outage_type, suburbs[], street_name, cause, etr, customers_off and a
    GeoJSON Polygon. It has no start time — only an estimated restore (etr).

Each scraper emits records in the shared polygon-outage shape consumed by
scrape.match_clients_to_outages (the same shape ausnet.py emits), so client
matching is pure point-in-polygon containment. Feeds are fetched through the
Nexy CRM AU-egress relay by default (they sit behind AWS/geo edges that block
GitHub's overseas runners), with the direct URLs as a local fallback.
"""
from __future__ import annotations

import math
import os
import re
from datetime import datetime, timezone, timedelta

import requests

# Operator base (Carrum Downs, VIC) + range — mirrors ausnet.py.
OPERATOR_BASE_LAT = -38.0833
OPERATOR_BASE_LNG = 145.1833
MAX_DISTANCE_KM = 200.0
MIN_CUSTOMERS = 10

MELBOURNE_TZ = timezone(timedelta(hours=10))
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

# Relay first (CRM Sydney), direct feed second (local AU runs).
_RELAY = os.environ.get("OUTAGE_RELAY_BASE", "https://crm.nexusenergy.au/api/outage-feed")
CPPC_SOURCES = [
    f"{_RELAY}?src=cppc",
    "https://s3-ap-southeast-2.amazonaws.com/cppc-outage/outages.json",
]
UE_SOURCES = [
    f"{_RELAY}?src=ue",
    "https://ds5ykmduea4ri.cloudfront.net/outages-v2.json",
]


def _haversine_km(lat1, lng1, lat2, lng2) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _fetch_json(sources: list[str]):
    """Try each source URL in order; return the first JSON success."""
    last = None
    for url in sources:
        try:
            r = requests.get(
                url,
                headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
                timeout=60,
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001
            last = e
            print(f"[vic] source failed ({url.split('?')[0]}: {e}); trying next", flush=True)
    raise RuntimeError(f"All sources failed: {last}")


def _polygon_from_geojson(geom):
    """GeoJSON Polygon/MultiPolygon -> [[lat, lng], ...] (feeds store [lng, lat])."""
    if not isinstance(geom, dict):
        return []
    coords = geom.get("coordinates")
    t = geom.get("type")
    if t == "Polygon" and coords:
        ring = coords[0]
    elif t == "MultiPolygon" and coords and coords[0]:
        ring = coords[0][0]
    else:
        return []
    pts = []
    for p in ring or []:
        if isinstance(p, (list, tuple)) and len(p) >= 2:
            try:
                pts.append([float(p[1]), float(p[0])])
            except (TypeError, ValueError):
                pass
    return pts if len(pts) >= 3 else []


def _centroid(polygon):
    return (sum(p[0] for p in polygon) / len(polygon),
            sum(p[1] for p in polygon) / len(polygon))


def _in_range(lat, lng):
    return _haversine_km(OPERATOR_BASE_LAT, OPERATOR_BASE_LNG, lat, lng) <= MAX_DISTANCE_KM


def _fmt_start(dt):
    return dt.strftime("%a %d %b, %I:%M %p").replace(" 0", " ") if dt else "Scheduled"


def _fmt_end(dt):
    return dt.strftime("%I:%M %p").lstrip("0") if dt else ""


def _record(polygon, start_dt, end_dt, network, incident_id, customers, suburb, street, status):
    duration = None
    if start_dt and end_dt:
        duration = round((end_dt - start_dt).total_seconds() / 3600.0, 2)
    return {
        "polygon": polygon,
        "polygon_centroid": _centroid(polygon),
        "start_dt": start_dt,
        "end_dt": end_dt,
        "start_display": _fmt_start(start_dt),
        "end_display": _fmt_end(end_dt),
        "start_iso": start_dt.isoformat() if start_dt else None,
        "end_iso": end_dt.isoformat() if end_dt else None,
        "status": status or "Scheduled",
        "duration_hours": duration,
        "network": network,
        "incident_id": str(incident_id),
        "customers": customers,
        "suburb": (suburb or "").title(),
        "street": street or "",
    }


# ---------------------------------------------------------------------------
# CitiPower + Powercor
# ---------------------------------------------------------------------------
def _parse_cppc_dt(s):
    """'08:00 08-07-2026' -> aware datetime, else None."""
    if not s:
        return None
    try:
        return datetime.strptime(s.strip(), "%H:%M %d-%m-%Y").replace(tzinfo=MELBOURNE_TZ)
    except (ValueError, AttributeError):
        return None


def scrape_cppc():
    data = _fetch_json(CPPC_SOURCES)
    rows = (data.get("ROWSET") or {}).get("ROW") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        raise RuntimeError("CPPC feed schema changed (no ROWSET.ROW list)")
    out = []
    now = datetime.now(MELBOURNE_TZ)
    for r in rows:
        if not isinstance(r, dict):
            continue
        cause = (r.get("CAUSE") or "").lower()
        # "planned" is a substring of "unplanned" — exclude explicitly.
        if "unplanned" in cause or "planned" not in cause:
            continue
        poly = _polygon_from_geojson(r.get("geometry"))
        if not poly:
            continue
        clat, clng = _centroid(poly)
        if not _in_range(clat, clng):
            continue
        try:
            customers = int(str(r.get("CUSTOMERS") or "0").strip() or 0)
        except ValueError:
            customers = 0
        start_dt = _parse_cppc_dt(r.get("START_TIME"))
        end_dt = _parse_cppc_dt(r.get("ETR"))
        if end_dt and end_dt < now - timedelta(days=1):
            continue  # long finished
        priv = (r.get("PRIVATISED") or "").strip()  # "WINGAN AVENUE, CAMBERWELL"
        street = priv.split(",")[0].strip() if priv else ""
        out.append(_record(
            poly, start_dt, end_dt,
            network=r.get("BUSINESS") or "Powercor",
            incident_id=r.get("ORDER_ID") or "",
            customers=customers,
            suburb=r.get("TOWN") or "",
            street=street,
            status=r.get("CREW_STATUS") or "Scheduled",
        ))
    print(f"[cppc] {len(out)} planned outages with polygons", flush=True)
    return out


# ---------------------------------------------------------------------------
# United Energy
# ---------------------------------------------------------------------------
def _parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).replace(tzinfo=MELBOURNE_TZ)
    except (ValueError, AttributeError):
        return None


def scrape_ue():
    data = _fetch_json(UE_SOURCES)
    outages = data.get("outages") if isinstance(data, dict) else None
    if not isinstance(outages, list):
        raise RuntimeError("United Energy feed schema changed (no outages list)")
    out = []
    now = datetime.now(MELBOURNE_TZ)
    for o in outages:
        if not isinstance(o, dict):
            continue
        if (o.get("outage_type") or "").lower() != "planned":
            continue
        poly = _polygon_from_geojson(o.get("geometry"))
        if not poly:
            continue
        clat, clng = _centroid(poly)
        if not _in_range(clat, clng):
            continue
        try:
            customers = int(o.get("customers_off") or 0)
        except (TypeError, ValueError):
            customers = 0
        # UE gives only an ETR (estimated restore); no scheduled start.
        end_dt = _parse_iso(o.get("etr"))
        if end_dt and end_dt < now - timedelta(days=1):
            continue
        suburbs = o.get("suburbs") or []
        out.append(_record(
            poly, None, end_dt,
            network="United Energy",
            incident_id=o.get("outage_id") or "",
            customers=customers,
            suburb=suburbs[0] if suburbs else "",
            street=(o.get("street_name") or "").strip(),
            status="Scheduled",
        ))
    print(f"[ue] {len(out)} planned outages with polygons", flush=True)
    return out


def scrape_vic_networks():
    """CitiPower + Powercor + United Energy, combined. Per-network failures are
    non-fatal so one distributor's feed hiccup never sinks the others."""
    combined = []
    for name, fn in (("cppc", scrape_cppc), ("ue", scrape_ue)):
        try:
            combined.extend(fn())
        except Exception as e:  # noqa: BLE001
            print(f"[vic] {name} scrape failed (continuing): {e}", flush=True)
    return combined
