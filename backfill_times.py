"""
One-off backfill script: populate the "Time Off - Time On" custom field on
existing Pipedrive leads, using the time data from today's scraped outages.

For each lead carrying the Jemena Planned Outage label:
  1. Parse client name and outage date from the lead title
  2. Look up the matching outage in today's scrape (matched by client + date)
  3. If found and the lead's time field is empty, patch with HH:MM:SS strings
  4. If not found, log and skip

Usage (via the manual_backfill workflow):
  Set 'dry_run' to true → log only, no API writes
  Set 'dry_run' to false → actually patch the leads

After backfill is complete, this file can be deleted.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
import traceback
from datetime import datetime, timezone, timedelta

# Reuse the existing pipeline pieces — same imports as scrape.py
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def parse_lead_title(title: str) -> tuple[str | None, str | None]:
    """'KFC Hillside Planned Power Outage - 2026-05-03' -> ('KFC Hillside', '2026-05-03')

    Tolerates the older 'KFC Planned Power Outage - 2026-05-03' (no decoration).
    """
    m = re.match(r"^(.+?)\s+Planned Power Outage\s*-\s*(\d{4}-\d{2}-\d{2})\s*$", title or "", re.IGNORECASE)
    if not m:
        return None, None
    return m.group(1).strip(), m.group(2).strip()


def to_24h_seconds(t: str) -> str:
    """Convert '7:30 AM' -> '07:30:00'. Returns '' if unparseable."""
    if not t:
        return ""
    t = t.strip().upper()
    is_pm = t.endswith("PM")
    is_am = t.endswith("AM")
    core = t[:-2].strip() if (is_pm or is_am) else t
    parts = core.split(":")
    try:
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError):
        return ""
    if is_am and hour == 12:
        hour = 0
    elif is_pm and hour != 12:
        hour += 12
    return f"{hour:02d}:{minute:02d}:00"


def main() -> int:
    # Run today's scrape so we have fresh outage data to match against.
    print("[backfill] running scrape pipeline to gather current outage data...", flush=True)
    import scrape
    # We need the affected list with all the time data, but we don't want to
    # re-create Pipedrive leads. Easiest: temporarily unset the API token
    # so the sync silently no-ops, then re-enable for our patches.
    saved_token = os.environ.pop("PIPEDRIVE_API_TOKEN", None)
    try:
        # Build the data the same way scrape.py main() does, but stop after
        # we have the affected list with time fields.
        raw_outages_for_lookup = _gather_outage_lookup()
    finally:
        if saved_token:
            os.environ["PIPEDRIVE_API_TOKEN"] = saved_token

    if not raw_outages_for_lookup:
        print("[backfill] no outage data available; aborting", flush=True)
        return 1

    print(f"[backfill] got {len(raw_outages_for_lookup)} (client, date) -> time entries", flush=True)

    # Now hit Pipedrive
    from pipedrive import PipedriveClient
    pd = PipedriveClient()
    if not pd.configured:
        print("[backfill] Pipedrive not configured (PIPEDRIVE_API_TOKEN missing)", flush=True)
        return 1
    pd.resolve_labels()
    if not pd.label_id:
        print("[backfill] could not resolve the Jemena Planned Outage label", flush=True)
        return 1
    if "time_off_on" not in pd.field_map:
        print("[backfill] time_off_on not in PIPEDRIVE_FIELD_MAP_JSON; cannot patch", flush=True)
        return 1

    time_field_key = pd.field_map["time_off_on"]
    time_until_key = f"{time_field_key}_until"

    # Page through all leads with the Jemena Planned Outage label
    print("[backfill] fetching existing leads from Pipedrive...", flush=True)
    leads = []
    start = 0
    limit = 100
    while True:
        try:
            res = pd._get("/leads", {
                "label_ids[]": pd.label_id,
                "archived_status": "all",
                "limit": limit,
                "start": start,
            })
        except Exception as e:
            print(f"[backfill] failed to fetch leads at offset {start}: {e}", flush=True)
            break
        batch = res.get("data") or []
        leads.extend(batch)
        pagination = (res.get("additional_data") or {}).get("pagination") or {}
        if not pagination.get("more_items_in_collection"):
            break
        start += limit
    print(f"[backfill] {len(leads)} total leads with the Jemena Planned Outage label", flush=True)

    counters = {
        "patched": 0,
        "already_set": 0,
        "no_match": 0,
        "unparseable_title": 0,
        "no_time_data": 0,
        "errors": 0,
    }
    no_match_examples = []

    for lead in leads:
        lead_id = lead.get("id")
        title = lead.get("title") or ""
        existing_off = lead.get(time_field_key)
        existing_until = lead.get(time_until_key)

        if existing_off and existing_until:
            counters["already_set"] += 1
            continue

        client_name, iso_date = parse_lead_title(title)
        if not client_name or not iso_date:
            counters["unparseable_title"] += 1
            continue

        # Look up matching outage. Try the full client name first, then the
        # un-decorated brand (in case the lead is from before suburb decoration).
        match = raw_outages_for_lookup.get((client_name.lower(), iso_date))
        if not match:
            # Try stripping last word (likely a suburb) to find an older lead format
            parts = client_name.rsplit(" ", 1)
            if len(parts) == 2:
                match = raw_outages_for_lookup.get((parts[0].lower(), iso_date))

        if not match:
            counters["no_match"] += 1
            if len(no_match_examples) < 10:
                no_match_examples.append(title)
            continue

        time_off = match.get("time_off_24")
        time_on = match.get("time_on_24")
        if not time_off or not time_on:
            counters["no_time_data"] += 1
            continue

        # Patch
        body = {
            time_field_key: time_off,
            time_until_key: time_on,
        }

        if pd.dry_run:
            print(f"[backfill] DRY: would patch {title!r} -> {time_off} - {time_on}", flush=True)
            counters["patched"] += 1
            continue

        try:
            pd._patch(f"/leads/{lead_id}", body)
            print(f"[backfill] patched {title!r} -> {time_off} - {time_on}", flush=True)
            counters["patched"] += 1
        except Exception as e:
            print(f"[backfill] failed to patch {title!r}: {e}", flush=True)
            counters["errors"] += 1

    print(f"\n[backfill] summary: {counters}", flush=True)
    if no_match_examples:
        print(f"[backfill] examples of leads with no matching current outage:", flush=True)
        for t in no_match_examples:
            print(f"  - {t}", flush=True)
    return 0


def _gather_outage_lookup() -> dict:
    """Run the scrape pipeline far enough to get outage time data,
    then build a lookup keyed by (client_name_lower, iso_date)."""
    import scrape
    from datetime import datetime as _dt
    # Scrape Jemena
    raw_outages = asyncio.run(scrape.scrape_outages())

    # Scrape Ausnet
    ausnet_outages = []
    try:
        from ausnet import scrape_ausnet
        ausnet_outages = scrape_ausnet()
    except Exception as e:
        print(f"[backfill] ausnet scrape failed (continuing without): {e}", flush=True)

    # Build a (client_name_lower, iso_date) -> time_off/time_on lookup.
    # The challenge: at this stage we have outages tied to suburb+street, not
    # to clients yet. We need to run the matching to associate them.
    suburb_streets = {}
    for o in raw_outages:
        suburb_streets.setdefault(o["suburb"], set()).add(o["street"])
    suburb_streets = {k: sorted(v) for k, v in suburb_streets.items()}

    suburb_geo = {}
    for sub in sorted(suburb_streets):
        loc = scrape.geocode_suburb(sub)
        if loc:
            suburb_geo[sub] = loc
    if not suburb_geo:
        return {}

    overpass = scrape.fetch_overpass(scrape.build_streets_query(suburb_streets, suburb_geo))

    outages_by_pair = {}
    for o in raw_outages:
        outages_by_pair.setdefault((o["suburb"].lower(), scrape.norm_key(o["street"])), []).append(o)
    pairs_by_street = {}
    for (suburb_l, street_n), outs in outages_by_pair.items():
        g = suburb_geo.get(suburb_l.upper())
        if g:
            pairs_by_street.setdefault(street_n, []).append((suburb_l.title(), g["lat"], g["lng"]))
    streets = scrape.match_streets(overpass, outages_by_pair, pairs_by_street)

    clients = scrape.assemble_clients()
    affected = scrape.match_clients_to_outages(clients, streets, raw_outages, ausnet_outages)

    # Apply suburb decoration so client names match what's in the lead title
    for a in affected:
        client = a["client"]
        if client.get("category") == "Shopping centre":
            continue
        brand = client.get("brand") or client.get("name", "")
        current = client.get("name", "")
        if current.lower() != brand.lower():
            continue
        candidates = a.get("definite", []) + a.get("possible", [])
        if not candidates:
            continue
        if a.get("definite"):
            outage_suburb = a["definite"][0]["suburb"]
        else:
            sorted_possible = sorted(
                a["possible"],
                key=lambda o: o.get("_distance_m") if o.get("_distance_m") is not None else float("inf"),
            )
            outage_suburb = sorted_possible[0]["suburb"]
        if outage_suburb and not scrape._name_already_includes(current, outage_suburb):
            sub = outage_suburb if not outage_suburb.isupper() else outage_suburb.title()
            client["name"] = f"{brand} {sub}"

    # Build the lookup map. For each affected client, group outages by date and
    # store the longest single outage's times as canonical for that date.
    from pipedrive import _to_24h
    lookup = {}
    for a in affected:
        client = a["client"]
        client_name = (client.get("name") or "").lower()
        all_outages = a.get("definite", []) + a.get("possible", [])
        by_date = {}
        for o in all_outages:
            # Skip cancelled — won't help backfill
            if o.get("status", "").lower() == "cancelled":
                continue
            iso = _iso_from_display(o.get("start", ""))
            if not iso:
                continue
            by_date.setdefault(iso, []).append(o)
        for iso_date, outages in by_date.items():
            # Pick the one with longest duration
            longest = max(outages, key=lambda o: float(o.get("duration_hours") or 0))
            t_off_display = _extract_time_only(longest.get("start", ""))
            t_on_display = longest.get("end", "")
            t_off_24 = _to_24h(t_off_display)
            t_on_24 = _to_24h(t_on_display)
            t_off_full = f"{t_off_24}:00" if t_off_24 and t_off_24.count(":") == 1 else t_off_24
            t_on_full = f"{t_on_24}:00" if t_on_24 and t_on_24.count(":") == 1 else t_on_24
            lookup[(client_name, iso_date)] = {
                "time_off_24": t_off_full,
                "time_on_24": t_on_full,
            }
    return lookup


def _iso_from_display(start_display: str) -> str:
    """'Tue 28 Apr, 7:30 AM' -> '2026-04-28' (year inferred from current year, +/-)"""
    if not start_display:
        return ""
    parts = start_display.split(",")
    if not parts:
        return ""
    date_part = parts[0].strip()
    m = re.match(r"\w+\s+(\d{1,2})\s+(\w+)", date_part)
    if not m:
        return ""
    day = int(m.group(1))
    month_name = m.group(2)[:3].title()
    months = {x: i for i, x in enumerate(
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}
    month = months.get(month_name)
    if not month:
        return ""
    today = datetime.now(timezone(timedelta(hours=10))).date()
    for year in [today.year, today.year + 1, today.year - 1]:
        try:
            d = datetime(year, month, day).date()
        except ValueError:
            continue
        if -30 <= (d - today).days <= 200:
            return d.strftime("%Y-%m-%d")
    return ""


def _extract_time_only(start_display: str) -> str:
    """'Tue 28 Apr, 7:30 AM' -> '7:30 AM'."""
    if not start_display:
        return ""
    parts = start_display.split(",")
    return parts[-1].strip() if len(parts) >= 2 else start_display


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"[backfill] FATAL: {e}", flush=True)
        traceback.print_exc()
        sys.exit(1)
