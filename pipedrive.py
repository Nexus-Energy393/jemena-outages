"""
Pipedrive integration for the Jemena outage map.

Creates/updates leads for affected clients that meet the duration threshold,
deduplicates by exact title match, and applies a 'Cancelled' label + activity
note when an outage is cancelled rather than deleting the lead. Archives
leads automatically once the outage has passed (with grace period).

Configuration reads from environment variables, set via GitHub repo secrets:

    PIPEDRIVE_API_TOKEN     required - your personal API token
    PIPEDRIVE_DOMAIN        optional - 'companyname' from companyname.pipedrive.com
                                       (default 'nexusenergy')
    PIPEDRIVE_DRY_RUN       optional - 'true'/'false' (default 'true' for safety)
    PIPEDRIVE_MIN_HOURS     optional - default 6.0
    PIPEDRIVE_OWNER_ID      optional - user ID to own created leads
    PIPEDRIVE_LABEL_NAME    optional - lead label name (default 'PLANNED POWER OUTAGE')
    PIPEDRIVE_CANCELLED_LABEL_NAME  optional - default 'CANCELLED'
    PIPEDRIVE_ARCHIVED_LABEL_NAME   optional - default 'AUTO-ARCHIVED'
    PIPEDRIVE_ARCHIVE_AFTER_DAYS    optional - days after outage to archive (default 2)
    PIPEDRIVE_FIELD_MAP_JSON  required - JSON {"site_address": "abc123...", ...}

Field map keys we support:
    site_address, locations, planned_outage_date, time_off_on,
    type, incident_id

Run with PIPEDRIVE_DRY_RUN=true (default) to log what WOULD happen without
making API calls. Set to false in repo secrets when you're ready.
"""
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import requests


PIPEDRIVE_BASE = "https://{domain}.pipedrive.com/api/v1"
DEFAULT_LABEL = "Jemena Planned Outage"
DEFAULT_CANCELLED_LABEL = "CANCELLED"
DEFAULT_ARCHIVED_LABEL = "AUTO-ARCHIVED"
DEFAULT_NOT_AFFECTED_LABEL = "Not Affected"
DEFAULT_DOMAIN = "nexusenergy"
MELBOURNE_TZ = timezone(timedelta(hours=10))


class PipedriveClient:
    def __init__(self):
        self.token = os.environ.get("PIPEDRIVE_API_TOKEN", "").strip()
        self.domain = os.environ.get("PIPEDRIVE_DOMAIN", DEFAULT_DOMAIN).strip()
        self.dry_run = os.environ.get("PIPEDRIVE_DRY_RUN", "true").lower() != "false"
        self.min_hours = float(os.environ.get("PIPEDRIVE_MIN_HOURS", "6.0"))
        self.owner_id = os.environ.get("PIPEDRIVE_OWNER_ID", "").strip() or None
        self.label_name = os.environ.get("PIPEDRIVE_LABEL_NAME", DEFAULT_LABEL).strip()
        self.cancelled_label_name = os.environ.get(
            "PIPEDRIVE_CANCELLED_LABEL_NAME", DEFAULT_CANCELLED_LABEL).strip()
        self.archived_label_name = os.environ.get(
            "PIPEDRIVE_ARCHIVED_LABEL_NAME", DEFAULT_ARCHIVED_LABEL).strip()
        self.not_affected_label_name = os.environ.get(
            "PIPEDRIVE_NOT_AFFECTED_LABEL_NAME", DEFAULT_NOT_AFFECTED_LABEL).strip()
        self.archive_after_days = int(os.environ.get("PIPEDRIVE_ARCHIVE_AFTER_DAYS", "2"))
        self.map_base_url = os.environ.get("MAP_BASE_URL",
            "https://nexus-energy393.github.io/jemena-outages").rstrip("/")
        try:
            self.field_map = json.loads(os.environ.get("PIPEDRIVE_FIELD_MAP_JSON", "{}"))
        except json.JSONDecodeError:
            print("[pipedrive] PIPEDRIVE_FIELD_MAP_JSON is not valid JSON; "
                  "custom fields will be skipped", flush=True)
            self.field_map = {}
        self.base = PIPEDRIVE_BASE.format(domain=self.domain)
        self.label_id = None
        self.cancelled_label_id = None
        self.archived_label_id = None
        self.not_affected_label_id = None
        self._session = requests.Session()

    @property
    def configured(self) -> bool:
        return bool(self.token) and bool(self.domain)

    # ------------------------------------------------------------------
    # Low-level HTTP
    # ------------------------------------------------------------------
    @staticmethod
    def _err_body(response) -> str:
        """Extract a readable error body from a failed Pipedrive response."""
        try:
            j = response.json()
            # Pipedrive returns errors like {"success": false, "error": "...", "error_info": "..."}
            parts = []
            if j.get("error"):
                parts.append(j["error"])
            if j.get("error_info"):
                parts.append(j["error_info"])
            if j.get("data") and isinstance(j["data"], dict) and j["data"].get("errors"):
                parts.append(json.dumps(j["data"]["errors"]))
            return " · ".join(parts) or json.dumps(j)[:500]
        except Exception:
            return (response.text or "")[:500]

    def _get(self, path: str, params: dict = None) -> dict:
        params = dict(params or {})
        params["api_token"] = self.token
        r = self._session.get(f"{self.base}{path}", params=params, timeout=30)
        if not r.ok:
            raise requests.HTTPError(f"{r.status_code} {self._err_body(r)}", response=r)
        return r.json()

    def _post(self, path: str, body: dict) -> dict:
        r = self._session.post(
            f"{self.base}{path}",
            params={"api_token": self.token},
            json=body,
            timeout=30,
        )
        if not r.ok:
            raise requests.HTTPError(f"{r.status_code} {self._err_body(r)}", response=r)
        return r.json()

    def _patch(self, path: str, body: dict) -> dict:
        r = self._session.patch(
            f"{self.base}{path}",
            params={"api_token": self.token},
            json=body,
            timeout=30,
        )
        if not r.ok:
            raise requests.HTTPError(f"{r.status_code} {self._err_body(r)}", response=r)
        return r.json()

    # ------------------------------------------------------------------
    # Labels
    # ------------------------------------------------------------------
    def resolve_labels(self) -> None:
        """Resolve label-name → ID, creating the label if needed."""
        try:
            res = self._get("/leadLabels")
            existing = res.get("data") or []
        except Exception as e:
            print(f"[pipedrive] could not fetch labels: {e}", flush=True)
            return

        for needed_attr, needed_name, color in [
            ("label_id", self.label_name, "red"),
            ("cancelled_label_id", self.cancelled_label_name, "gray"),
            ("archived_label_id", self.archived_label_name, "blue"),
            ("not_affected_label_id", self.not_affected_label_name, "green"),
        ]:
            match = next((l for l in existing if l.get("name") == needed_name), None)
            if match:
                setattr(self, needed_attr, match["id"])
                continue
            if self.dry_run:
                print(f"[pipedrive] DRY: would create label {needed_name!r}", flush=True)
                continue
            try:
                created = self._post("/leadLabels", {"name": needed_name, "color": color})
                setattr(self, needed_attr, created["data"]["id"])
            except Exception as e:
                print(f"[pipedrive] failed to create label {needed_name!r}: {e}", flush=True)

    # ------------------------------------------------------------------
    # Person / Organisation upsert
    # ------------------------------------------------------------------
    def upsert_person(self, client: dict) -> int | None:
        """Look up by email/phone; create if missing. Returns Pipedrive person ID.

        Returns None (no person attached to lead) if the client has no real
        contact info — we don't want placeholder People named after their
        organisation cluttering up Pipedrive's contact list.
        """
        contact_name = (client.get("contact_name") or "").strip()
        email = (client.get("contact_email") or "").strip()
        phone = (client.get("contact_phone") or "").strip()

        # No real contact info? Skip person creation entirely.
        if not contact_name and not email and not phone:
            return None

        # Try to find by email first, then phone
        for term, field in [(email, "email"), (phone, "phone")]:
            if not term:
                continue
            try:
                res = self._get("/persons/search", {"term": term, "fields": field, "limit": 1})
                items = res.get("data", {}).get("items") or []
                if items:
                    return items[0]["item"]["id"]
            except Exception:
                pass

        # Create with the contact name (or fall back to org name only as a last resort)
        body = {"name": contact_name or client.get("name") or "Contact"}
        if email:
            body["email"] = [{"value": email, "primary": True}]
        if phone:
            body["phone"] = [{"value": phone, "primary": True}]
        if self.owner_id:
            body["owner_id"] = int(self.owner_id)

        if self.dry_run:
            print(f"[pipedrive] DRY: would create Person {body['name']!r} for {client.get('name')!r}", flush=True)
            return None
        try:
            created = self._post("/persons", body)
            return created["data"]["id"]
        except Exception as e:
            print(f"[pipedrive] failed to create person {body['name']!r}: {e}", flush=True)
            return None

    def upsert_organisation(self, client: dict) -> int | None:
        org_name = client.get("name") or "Unknown"
        # Search
        try:
            res = self._get("/organizations/search", {"term": org_name, "fields": "name", "limit": 1})
            items = res.get("data", {}).get("items") or []
            if items and items[0]["item"].get("name", "").lower() == org_name.lower():
                return items[0]["item"]["id"]
        except Exception:
            pass

        body = {"name": org_name}
        if self.owner_id:
            body["owner_id"] = int(self.owner_id)
        if self.dry_run:
            print(f"[pipedrive] DRY: would create Organisation {org_name!r}", flush=True)
            return None
        try:
            created = self._post("/organizations", body)
            return created["data"]["id"]
        except Exception as e:
            print(f"[pipedrive] failed to create organisation {org_name!r}: {e}", flush=True)
            return None

    # ------------------------------------------------------------------
    # Lead lookup / create / update
    # ------------------------------------------------------------------
    def find_lead_by_title(self, title: str) -> dict | None:
        """Pipedrive's /leads endpoint has no title-search, but /itemSearch does."""
        try:
            res = self._get(
                "/itemSearch",
                {"term": title, "item_types": "lead", "fields": "title", "exact_match": "true", "limit": 1},
            )
            items = res.get("data", {}).get("items") or []
            if items:
                return items[0].get("item")
        except Exception as e:
            print(f"[pipedrive] lead search failed for {title!r}: {e}", flush=True)
        return None

    def build_custom_fields(self, opp: dict) -> dict:
        """opp is the OpportunityRecord dict (see prepare_opportunities)."""
        cf = {}
        fmap = self.field_map
        # Only set fields with non-empty values; Pipedrive rejects some
        # field types when given empty strings.
        if "site_address" in fmap and opp.get("site_address"):
            cf[fmap["site_address"]] = opp["site_address"]
        if "locations" in fmap and opp.get("locations"):
            cf[fmap["locations"]] = opp["locations"]
        if "planned_outage_date" in fmap and opp.get("planned_outage_date"):
            cf[fmap["planned_outage_date"]] = opp["planned_outage_date"]
        # Map Link: deep-link back to the relevant marker on the live map
        if "map_link" in fmap and opp.get("client", {}).get("client_id"):
            cf[fmap["map_link"]] = f"{self.map_base_url}/?focus={opp['client']['client_id']}"
        # Pipedrive's "Time Off - Time On" timeRange field has a strict
        # format we can't reliably populate via the v1 API. Set
        # PIPEDRIVE_TIME_FORMAT in env to override:
        #   "skip"   - don't send the field (default; safe)
        #   "string" - send "07:00 - 17:30"
        #   "object" - send {"from","until","timezone_id"} (only works on some accounts)
        time_format = os.environ.get("PIPEDRIVE_TIME_FORMAT", "skip").lower().strip()
        if "time_off_on" in fmap and time_format != "skip":
            t_off = opp.get("time_off") or ""
            t_on = opp.get("time_on") or ""
            if t_off and t_on:
                if time_format == "object":
                    cf[fmap["time_off_on"]] = {
                        "from": t_off,
                        "until": t_on,
                        "timezone_id": "Australia/Melbourne",
                    }
                else:  # 'string'
                    cf[fmap["time_off_on"]] = f"{t_off} - {t_on}"
        # Type is a Pipedrive dropdown (single-option) field — needs numeric
        # option ID, not text. Set PIPEDRIVE_TYPE_OPTION_ID in env to populate.
        type_option_id = os.environ.get("PIPEDRIVE_TYPE_OPTION_ID", "").strip()
        if "type" in fmap and type_option_id:
            try:
                cf[fmap["type"]] = int(type_option_id)
            except ValueError:
                pass  # ignore non-numeric values
        # Skip incident_id when it's empty — Pipedrive may reject empty values
        # on certain field types (e.g. numeric).
        if "incident_id" in fmap and opp.get("incident_id"):
            cf[fmap["incident_id"]] = opp["incident_id"]
        return cf

    def create_lead(self, opp: dict) -> dict | None:
        title = opp["title"]
        body = {"title": title}

        person_id = self.upsert_person(opp["client"])
        org_id = self.upsert_organisation(opp["client"])
        if person_id:
            body["person_id"] = person_id
        if org_id:
            body["organization_id"] = org_id

        # Pipedrive requires at least one of person_id or organization_id.
        # If both creation attempts failed, skip rather than fail.
        if not person_id and not org_id:
            print(f"[pipedrive] skipping {title!r}: could not create person or organisation", flush=True)
            return None

        if self.owner_id:
            body["owner_id"] = int(self.owner_id)
        if self.label_id:
            body["label_ids"] = [self.label_id]

        custom_fields = self.build_custom_fields(opp)
        if custom_fields:
            body.update(custom_fields)

        if self.dry_run:
            print(f"[pipedrive] DRY: would create Lead {title!r}", flush=True)
            print(f"            body keys: {list(body.keys())}", flush=True)
            return None
        try:
            created = self._post("/leads", body)
            print(f"[pipedrive] created lead {title!r}", flush=True)
            return created.get("data")
        except Exception as e:
            print(f"[pipedrive] failed to create lead {title!r}: {e}", flush=True)
            print(f"            body keys: {list(body.keys())}", flush=True)
            return None

    def mark_lead_cancelled(self, lead: dict, opp: dict) -> None:
        """Apply Cancelled label and add an activity/note if not already there."""
        lead_id = lead.get("id")
        title = lead.get("title")
        if not lead_id:
            return
        # Already cancelled?
        labels = lead.get("label_ids") or []
        if self.cancelled_label_id and self.cancelled_label_id in labels:
            return  # nothing to do

        new_labels = list(labels)
        if self.cancelled_label_id and self.cancelled_label_id not in new_labels:
            new_labels.append(self.cancelled_label_id)

        if self.dry_run:
            print(f"[pipedrive] DRY: would mark lead {title!r} as Cancelled", flush=True)
            return
        try:
            self._patch(f"/leads/{lead_id}", {"label_ids": new_labels})
        except Exception as e:
            print(f"[pipedrive] failed to update lead labels for {title!r}: {e}", flush=True)
            return

        # Add a timestamped note for audit trail
        try:
            now_str = datetime.now(MELBOURNE_TZ).strftime("%Y-%m-%d %H:%M AEST")
            self._post("/notes", {
                "lead_id": lead_id,
                "content": (
                    f"Outage cancelled by Jemena (detected {now_str}). "
                    f"This lead was auto-flagged. Decide whether to close-lost."
                ),
            })
            print(f"[pipedrive] marked {title!r} cancelled", flush=True)
        except Exception as e:
            print(f"[pipedrive] failed to add cancellation note: {e}", flush=True)

    def fetch_not_affected_client_ids(self) -> set[str]:
        """Return the set of client_ids the rep has marked as 'Not Affected'.

        We extract the client_id from the Map Link URL custom field value.
        That URL looks like .../?focus=<client-id>, set when the lead was
        created. So if the rep applies the 'Not Affected' label, we know
        which client to exclude from tomorrow's affected list.
        """
        if not self.not_affected_label_id:
            return set()
        map_link_field = self.field_map.get("map_link")
        if not map_link_field:
            print("[pipedrive] map_link field not mapped; cannot read Not Affected exclusions", flush=True)
            return set()

        excluded: set[str] = set()
        try:
            start = 0
            limit = 100
            while True:
                res = self._get("/leads", {
                    "label_ids[]": self.not_affected_label_id,
                    "archived_status": "all",
                    "limit": limit,
                    "start": start,
                })
                leads = res.get("data") or []
                for lead in leads:
                    map_url = lead.get(map_link_field) or ""
                    # Extract ?focus=<id> from the URL
                    m = re.search(r"[?&]focus=([^&\s]+)", map_url)
                    if m:
                        excluded.add(m.group(1))
                pagination = (res.get("additional_data") or {}).get("pagination") or {}
                if not pagination.get("more_items_in_collection"):
                    break
                start += limit
        except Exception as e:
            print(f"[pipedrive] failed to fetch Not Affected leads: {e}", flush=True)
            return set()

        if excluded:
            print(f"[pipedrive] excluding {len(excluded)} client(s) marked Not Affected: "
                  f"{sorted(excluded)[:5]}{'...' if len(excluded) > 5 else ''}", flush=True)
        return excluded

    def archive_stale_leads(self) -> int:
        """Archive leads where the outage has passed.

        Selection criteria:
            - Lead carries our PLANNED POWER OUTAGE label (created by us)
            - Planned Outage Date custom field is more than `archive_after_days` ago
            - Lead is not already archived
            - Lead has not been converted to a deal (still in the inbox)

        Applies AUTO-ARCHIVED label and flips is_archived=true.
        Returns the number archived.
        """
        outage_date_field = self.field_map.get("planned_outage_date")
        if not outage_date_field:
            print("[pipedrive] no planned_outage_date field mapped; cannot archive", flush=True)
            return 0

        cutoff = (datetime.now(MELBOURNE_TZ) - timedelta(days=self.archive_after_days)).date()
        cutoff_str = cutoff.strftime("%Y-%m-%d")

        # Pipedrive's /leads endpoint supports filtering by label
        if not self.label_id:
            print("[pipedrive] no PLANNED POWER OUTAGE label resolved; cannot archive", flush=True)
            return 0

        archived_count = 0
        skipped_active = 0
        try:
            # Paginate through leads with our label
            start = 0
            limit = 100
            while True:
                res = self._get("/leads", {
                    "label_ids[]": self.label_id,
                    "archived_status": "not_archived",
                    "limit": limit,
                    "start": start,
                })
                leads = res.get("data") or []
                for lead in leads:
                    outage_date = lead.get(outage_date_field) or ""
                    if not outage_date:
                        continue
                    # Pipedrive returns date fields as 'YYYY-MM-DD'
                    if outage_date >= cutoff_str:
                        continue  # outage is in the future or within grace period

                    # Check it's not already auto-archived (idempotent)
                    labels = lead.get("label_ids") or []
                    if self.archived_label_id and self.archived_label_id in labels:
                        continue

                    title = lead.get("title", "")
                    lead_id = lead.get("id")

                    if self.dry_run:
                        print(f"[pipedrive] DRY: would archive {title!r} (outage was {outage_date})", flush=True)
                        archived_count += 1
                        continue

                    new_labels = list(labels)
                    if self.archived_label_id and self.archived_label_id not in new_labels:
                        new_labels.append(self.archived_label_id)

                    try:
                        self._patch(f"/leads/{lead_id}", {
                            "label_ids": new_labels,
                            "is_archived": True,
                        })
                        print(f"[pipedrive] archived {title!r} (outage was {outage_date})", flush=True)
                        archived_count += 1
                    except Exception as e:
                        print(f"[pipedrive] failed to archive {title!r}: {e}", flush=True)

                # Pagination
                pagination = (res.get("additional_data") or {}).get("pagination") or {}
                if not pagination.get("more_items_in_collection"):
                    break
                start += limit
        except Exception as e:
            print(f"[pipedrive] archive sweep failed: {e}", flush=True)

        return archived_count


# ---------------------------------------------------------------------------
# Opportunity preparation (run before any API calls)
# ---------------------------------------------------------------------------
def prepare_opportunities(affected, min_hours):
    """Walk the affected list, output one opportunity per (client, distinct outage day).

    A client with multiple outages on different days produces multiple
    opportunities (so the title carries the date, matching the existing
    workflow in your screenshot). Cancelled-only clients still get
    opportunities so the cancellation can be reflected on existing leads.
    """
    opportunities = []
    for a in affected:
        client = a["client"]
        all_outages = a.get("definite", []) + a.get("possible", [])
        if not all_outages:
            continue

        # Group outages by date
        by_date: dict[str, list[dict]] = {}
        for o in all_outages:
            # 'start' is e.g. 'Tue 28 Apr, 7:30 AM' - extract just the date part
            date_part = (o.get("start") or "").split(",")[0].strip()
            by_date.setdefault(date_part, []).append(o)

        for date_part, outages in by_date.items():
            active = [o for o in outages if o.get("status", "").lower() != "cancelled"]
            cancelled_only = not active

            # Determine if this opportunity meets the threshold
            durations = [float(o.get("duration_hours") or 0) for o in (active or outages)]
            longest = max(durations) if durations else 0.0

            if not cancelled_only and longest < min_hours:
                continue  # below threshold and still scheduled — skip

            # Title format mirrors existing manual workflow
            client_name = client.get("name") or "Unknown"
            iso_date = _format_date_for_title(date_part, outages[0])
            title = f"{client_name} Planned Power Outage - {iso_date}"

            # Build a single representative summary
            primary = sorted(outages, key=lambda o: o.get("_distance_m") or 99999)[0]
            time_off = _extract_time(primary.get("start"), "time_only")
            time_on = primary.get("end") or ""
            # Parse to 24h HH:MM for Pipedrive's timeRange field
            time_off_24 = _to_24h(time_off)
            time_on_24 = _to_24h(time_on)
            opp = {
                "client": client,
                "title": title,
                "outage_date": iso_date,
                "all_outages": outages,
                "active_outages": active,
                "cancelled_only": cancelled_only,
                "longest_hours": longest,
                # Custom-field-ready values
                "site_address": _format_site_address(client),
                "locations": ", ".join(sorted({o.get("suburb", "") for o in outages})),
                "planned_outage_date": iso_date,
                # Pipedrive timeRange expects {"from", "until", "timezone_id"} structure
                "time_off": time_off_24,
                "time_on": time_on_24,
                "type": "Planned",
                "incident_id": "",
            }
            opportunities.append(opp)
    return opportunities


def _format_date_for_title(date_str, outage):
    """Convert 'Tue 28 Apr' -> '2026-04-28'. We need the year, which isn't in
    the display string, so derive from the original outage if possible.

    For now we use today's year as a fallback; year boundary edge cases will
    be wrong for at most a few days a year and only matter for title lookup."""
    try:
        # 'Tue 28 Apr' -> day, month
        parts = date_str.split()
        day = int(parts[1])
        month_str = parts[2][:3]
        months = {m: i for i, m in enumerate(
            ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
             "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}
        month = months.get(month_str)
        if not month:
            return date_str
        today = datetime.now(MELBOURNE_TZ).date()
        # Pick the closest year that puts the date within ~6 months
        for year in [today.year, today.year + 1]:
            try:
                d = datetime(year, month, day).date()
            except ValueError:
                continue
            if -30 <= (d - today).days <= 200:
                return d.strftime("%Y-%m-%d")
        return f"{today.year}-{month:02d}-{day:02d}"
    except Exception:
        return date_str


def _format_site_address(client):
    parts = [
        client.get("address", ""),
        client.get("suburb", ""),
        client.get("postcode", ""),
    ]
    return ", ".join(p for p in parts if p)


def _extract_time(start_display, mode):
    """Extract '7:30 AM' from 'Tue 28 Apr, 7:30 AM'."""
    if not start_display:
        return ""
    parts = start_display.split(",")
    if len(parts) >= 2:
        return parts[-1].strip()
    return start_display


def _to_24h(t):
    """Convert '7:30 AM' -> '07:30' for Pipedrive's timeRange field.

    Returns empty string if the input can't be parsed.
    """
    if not t:
        return ""
    t = t.strip().upper()
    # Handle 'AM'/'PM' suffix
    is_pm = t.endswith("PM")
    is_am = t.endswith("AM")
    core = t[:-2].strip() if (is_pm or is_am) else t
    # Split hour:minute
    parts = core.split(":")
    try:
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError):
        return ""
    # 12 AM = 00, 12 PM = 12, 1-11 PM = +12
    if is_am and hour == 12:
        hour = 0
    elif is_pm and hour != 12:
        hour += 12
    return f"{hour:02d}:{minute:02d}"


# ---------------------------------------------------------------------------
# Top-level orchestration (called from scrape.py)
# ---------------------------------------------------------------------------
def sync_to_pipedrive(affected):
    """Push opportunities to Pipedrive. Safe to call regardless of config:
    silently no-ops if PIPEDRIVE_API_TOKEN isn't set."""
    pd = PipedriveClient()
    if not pd.configured:
        print("[pipedrive] not configured (no PIPEDRIVE_API_TOKEN set); skipping", flush=True)
        return {"created": 0, "updated": 0, "cancelled": 0, "skipped": 0, "archived": 0, "configured": False}

    print(f"[pipedrive] domain={pd.domain} dry_run={pd.dry_run} "
          f"min_hours={pd.min_hours}", flush=True)

    pd.resolve_labels()

    # Fetch client_ids the rep has marked Not Affected. Skip those everywhere.
    not_affected = pd.fetch_not_affected_client_ids()

    opps = prepare_opportunities(affected, pd.min_hours)
    # Filter out opportunities for not-affected clients
    pre_count = len(opps)
    opps = [o for o in opps if o["client"].get("client_id") not in not_affected]
    if len(opps) < pre_count:
        print(f"[pipedrive] {pre_count - len(opps)} opportunities skipped due to Not Affected label", flush=True)
    print(f"[pipedrive] {len(opps)} opportunities prepared", flush=True)

    counters = {"created": 0, "updated": 0, "cancelled": 0, "skipped": 0,
                "archived": 0, "not_affected_excluded": pre_count - len(opps),
                "configured": True}
    for opp in opps:
        existing = pd.find_lead_by_title(opp["title"])
        if existing:
            if opp["cancelled_only"]:
                pd.mark_lead_cancelled(existing, opp)
                counters["cancelled"] += 1
            else:
                # Lead already exists for an active outage — leave it alone.
                # (Could PATCH custom fields if Jemena rescheduled times, but
                # that risks overwriting rep edits. Leave it.)
                counters["skipped"] += 1
        elif opp["cancelled_only"]:
            # No existing lead and the outage was cancelled — don't create.
            counters["skipped"] += 1
        else:
            pd.create_lead(opp)
            counters["created"] += 1

    # Archive stale leads (outage has passed)
    counters["archived"] = pd.archive_stale_leads()

    # Make the not-affected client list available to the caller so the map
    # can also exclude those clients from the affected layer.
    counters["not_affected_client_ids"] = sorted(not_affected)

    print(f"[pipedrive] summary: {counters}", flush=True)
    return counters
