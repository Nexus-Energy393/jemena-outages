"""
Nexy CRM integration for the outages map — the sole leads sink (replaces the
retired Pipedrive module).

For every affected client that meets the duration threshold, POSTs an enriched
planned-outage opportunity to the Nexy Leads inbox:

    POST https://crm.nexusenergy.au/api/intake/outage-lead
    header: x-nexy-intake-secret: <NEXY_INTAKE_SECRET>

Idempotent + two-way:
  * externalId = "<client_id>::<incident_key>" — stable per client per outage,
    so re-runs upsert. The CRM refreshes only scraper-owned outage fields and
    never clobbers CRM-side state (status, owner, links, archive, deal).
  * Cancelled outages are pushed too; the CRM closes the matching lead as
    NOT_AFFECTED (and never creates leads for cancelled-only rows).
  * After pushing, statuses are read back (GET ?ids=…) so the map can deep-link
    each client to its CRM lead and hide clients the rep dismissed.

Configuration (GitHub repo secrets / vars):

    NEXY_INTAKE_SECRET    required - the CRM's INTAKE_WEBHOOK_SECRET
    NEXY_INTAKE_URL       optional - default https://crm.nexusenergy.au/api/intake/outage-lead
    NEXY_DRY_RUN          optional - 'true'/'false' (default 'true' for safety on
                                     local runs; the workflow passes 'false')
    NEXY_MIN_HOURS        optional - default 6.0 (per-client min_outage_hours wins)
    NEXY_MAX_DAYS_AHEAD   optional - default 21; leads further out aren't created
                                     (don't reach out before the utility notifies)
    MAP_BASE_URL          optional - default https://outages.nexusenergy.au
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone, timedelta

import requests


DEFAULT_INTAKE_URL = "https://crm.nexusenergy.au/api/intake/outage-lead"
MELBOURNE_TZ = timezone(timedelta(hours=10))
STATUS_BATCH = 60          # externalIds per GET (keeps the URL well under limits)
HTTP_TIMEOUT = 30

# CRM lead statuses that mean "the rep said this client isn't affected — hide it"
DISMISSED_STATUSES = {"DISMISSED", "DISQUALIFIED"}


class NexyClient:
    def __init__(self):
        self.secret = os.environ.get("NEXY_INTAKE_SECRET", "").strip()
        self.url = os.environ.get("NEXY_INTAKE_URL", DEFAULT_INTAKE_URL).strip().rstrip("/")
        self.dry_run = os.environ.get("NEXY_DRY_RUN", "true").lower() != "false"
        self.min_hours = float(os.environ.get("NEXY_MIN_HOURS", "6.0"))
        try:
            self.max_days_ahead = int(os.environ.get("NEXY_MAX_DAYS_AHEAD", "21"))
        except ValueError:
            self.max_days_ahead = 21
        self.map_base_url = os.environ.get("MAP_BASE_URL", "https://outages.nexusenergy.au").rstrip("/")
        self._session = requests.Session()

    @property
    def configured(self) -> bool:
        return bool(self.secret)

    def _headers(self) -> dict:
        return {"x-nexy-intake-secret": self.secret, "Content-Type": "application/json"}

    def push(self, payload: dict) -> dict:
        r = self._session.post(self.url, json=payload, headers=self._headers(), timeout=HTTP_TIMEOUT)
        try:
            body = r.json()
        except ValueError:
            body = {"ok": False, "message": (r.text or "")[:300]}
        if not r.ok and r.status_code != 404:
            raise requests.HTTPError(f"{r.status_code} {json.dumps(body)[:300]}", response=r)
        return body

    def statuses(self, external_ids: list[str]) -> dict:
        """GET the CRM-side state for a set of outage rows. Returns {externalId: {...}}."""
        out: dict[str, dict] = {}
        for i in range(0, len(external_ids), STATUS_BATCH):
            chunk = external_ids[i:i + STATUS_BATCH]
            r = self._session.get(
                self.url,
                params={"ids": ",".join(chunk)},
                headers={"x-nexy-intake-secret": self.secret},
                timeout=HTTP_TIMEOUT,
            )
            if not r.ok:
                print(f"[nexy] status read-back failed ({r.status_code}); continuing", flush=True)
                continue
            data = r.json() if r.content else {}
            for k, v in (data.get("statuses") or {}).items():
                out[k] = v
        return out


# ---------------------------------------------------------------------------
# Opportunity preparation
# ---------------------------------------------------------------------------

def _incident_key(outage: dict) -> str:
    """Stable per-outage key: real incident/event id, else the outage day."""
    for k in ("incident_id", "event_id"):
        v = outage.get(k)
        if v not in (None, ""):
            return str(v)
    iso = outage.get("start_iso") or ""
    day = iso[:10] if iso else re.sub(r"[^A-Za-z0-9]+", "-", str(outage.get("start") or "unknown"))
    return f"d:{day}"


def _match_basis(outage: dict, is_definite: bool) -> str:
    m = outage.get("_match")
    if m in ("in-zone", "listed-street-edge", "near-zone"):
        return m
    # Street/suburb exact matches (no polygon published) are strong; whole-street
    # proximity fallbacks are not.
    return "listed-street-edge" if is_definite else "near-zone"


def _time_from_display(display: str | None, part: str) -> str:
    """'Tue 28 Apr, 7:30 AM' -> '7:30 AM' (part='time')."""
    if not display:
        return ""
    if part == "time":
        bits = display.split(",")
        return bits[-1].strip() if len(bits) >= 2 else display.strip()
    return display.strip()


def _iso_date(outage: dict) -> str | None:
    iso = outage.get("start_iso")
    return iso[:10] if iso else None


def prepare_opportunities(affected, min_hours, max_days_ahead=None, map_base_url="https://outages.nexusenergy.au"):
    """One opportunity per (client, incident). Cancelled-only groups are still
    emitted (with outageStatus='Cancelled') so the CRM can close their leads."""
    today = datetime.now(MELBOURNE_TZ).date()
    skipped_too_far = 0
    skipped_below = 0
    opportunities = []

    for a in affected:
        client = a["client"]
        cid = client.get("client_id") or ""
        if not cid:
            continue

        # Group this client's outages by incident key, remembering definiteness.
        groups: dict[str, list[tuple[dict, bool]]] = {}
        for o in a.get("definite", []):
            groups.setdefault(_incident_key(o), []).append((o, True))
        for o in a.get("possible", []):
            groups.setdefault(_incident_key(o), []).append((o, False))

        client_min = client.get("min_outage_hours")
        try:
            threshold = float(client_min) if client_min not in (None, "") else min_hours
        except (ValueError, TypeError):
            threshold = min_hours

        for key, items in groups.items():
            outages = [o for o, _ in items]
            active = [o for o in outages if (o.get("status") or "").lower() != "cancelled"]
            cancelled_only = not active
            pool = active or outages
            durations = [float(o.get("duration_hours") or 0) for o in pool]
            longest = max(durations) if durations else 0.0

            if not cancelled_only and longest < threshold:
                skipped_below += 1
                continue

            # Prefer the strongest record for the representative fields.
            def rank(item):
                o, is_def = item
                m = o.get("_match")
                order = {"in-zone": 0, "listed-street-edge": 1, "near-zone": 3}.get(m, 1 if is_def else 3)
                return (order, o.get("_distance_m") or 0)
            primary, primary_definite = sorted(items, key=rank)[0]

            iso_date = _iso_date(primary)
            if max_days_ahead is not None and not cancelled_only and iso_date:
                try:
                    days_ahead = (datetime.strptime(iso_date, "%Y-%m-%d").date() - today).days
                    if days_ahead > max_days_ahead:
                        skipped_too_far += 1
                        continue
                except ValueError:
                    pass

            client_name = client.get("name") or "Unknown"
            title_date = iso_date or _time_from_display(primary.get("start"), "date")
            status = "Cancelled" if cancelled_only else (primary.get("status") or "Scheduled")

            payload = {
                "externalId": f"{cid}::{key}",
                "client": client_name,
                "company": client_name,
                "title": f"{client_name} Planned Power Outage - {title_date}",
                "category": client.get("category") or "",
                "suburb": client.get("suburb") or "",
                "postcode": str(client.get("postcode") or ""),
                "address": ", ".join(p for p in (client.get("address"), client.get("suburb"), str(client.get("postcode") or "")) if p),
                "incidentId": str(primary.get("incident_id") or primary.get("event_id") or ""),
                "outageDate": iso_date or "",
                "timeOff": _time_from_display(primary.get("start"), "time"),
                "timeOn": (primary.get("end") or "").strip(),
                "locations": ", ".join(sorted({o.get("suburb", "") for o in outages if o.get("suburb")})) or (client.get("suburb") or ""),
                "contactName": client.get("contact_name") or "",
                "contactPhone": str(client.get("contact_phone") or ""),
                "contactEmail": client.get("contact_email") or "",
                # ---- rich signals ----
                "network": primary.get("network") or "Jemena",
                "customersAffected": int(primary.get("customers")) if str(primary.get("customers") or "").isdigit() else None,
                "outageStartIso": primary.get("start_iso"),
                "outageEndIso": primary.get("end_iso"),
                "durationHours": round(longest, 2),
                "matchBasis": _match_basis(primary, primary_definite),
                "distanceM": primary.get("_distance_m"),
                "latitude": client.get("lat"),
                "longitude": client.get("lng"),
                "mapUrl": f"{map_base_url}/?focus={cid}",
                "generatorOpportunity": bool(longest >= threshold),
                "outageStatus": status,
            }
            # Drop empty-string / None optionals to keep the payload tight.
            payload = {k: v for k, v in payload.items() if v not in (None, "")}
            payload["_cancelled_only"] = cancelled_only  # internal, stripped before send
            payload["_client_id"] = cid
            opportunities.append(payload)

    if skipped_below:
        print(f"[nexy] {skipped_below} opportunities below duration threshold", flush=True)
    if skipped_too_far:
        print(f"[nexy] {skipped_too_far} opportunities skipped (outage > {max_days_ahead} days away)", flush=True)
    return opportunities


# ---------------------------------------------------------------------------
# Top-level orchestration (called from scrape.py)
# ---------------------------------------------------------------------------

def sync_to_nexy(affected):
    """Push opportunities to the Nexy CRM. Safe to call regardless of config:
    silently no-ops if NEXY_INTAKE_SECRET isn't set. Returns a summary dict
    compatible with the map/table decorations:
      created / updated / cancelled / skipped counts,
      client_lead_ids  {client_id: leadId} for deep links,
      not_affected_client_ids [client_id] the rep dismissed (hidden from map)."""
    nexy = NexyClient()
    summary = {"created": 0, "updated": 0, "cancelled": 0, "skipped": 0, "errors": 0,
               "configured": nexy.configured, "not_affected_client_ids": [], "client_lead_ids": {}}
    if not nexy.configured:
        print("[nexy] not configured (no NEXY_INTAKE_SECRET set); skipping", flush=True)
        return summary

    print(f"[nexy] url={nexy.url} dry_run={nexy.dry_run} min_hours={nexy.min_hours} "
          f"max_days_ahead={nexy.max_days_ahead}", flush=True)

    opps = prepare_opportunities(affected, nexy.min_hours, nexy.max_days_ahead, nexy.map_base_url)
    print(f"[nexy] {len(opps)} opportunities prepared", flush=True)
    if not opps:
        return summary

    ext_ids = [o["externalId"] for o in opps]

    # Pre-read CRM state: dismissed clients don't get NEW leads (their existing
    # rows still refresh so cancellations propagate).
    pre = {}
    if not nexy.dry_run:
        try:
            pre = nexy.statuses(ext_ids)
        except Exception as e:
            print(f"[nexy] pre-sync status read failed (continuing): {e}", flush=True)
    dismissed_clients = set()
    client_lead: dict[str, str] = {}
    for opp in opps:
        st = pre.get(opp["externalId"]) or {}
        if st.get("exists") and st.get("status") in DISMISSED_STATUSES:
            dismissed_clients.add(opp["_client_id"])

    for opp in opps:
        cid = opp.pop("_client_id")
        cancelled_only = opp.pop("_cancelled_only")
        st = pre.get(opp["externalId"]) or {}
        is_new = not st.get("exists")

        if is_new and cid in dismissed_clients and not cancelled_only:
            summary["skipped"] += 1
            continue

        if nexy.dry_run:
            verb = "close (cancelled)" if cancelled_only else ("create" if is_new else "refresh")
            print(f"[nexy] DRY: would {verb} {opp['externalId']} ({opp.get('title')})", flush=True)
            summary["created" if (is_new and not cancelled_only) else "updated"] += 1
            continue

        try:
            body = nexy.push(opp)
        except Exception as e:
            summary["errors"] += 1
            print(f"[nexy] push failed for {opp['externalId']}: {e}", flush=True)
            continue

        if body.get("leadId"):
            # Prefer an open lead's id for the map deep-link; any id beats none.
            if cid not in client_lead or body.get("status") not in ("NOT_AFFECTED", "EXPIRED"):
                client_lead[cid] = body["leadId"]
        if body.get("skipped") == "cancelled-no-lead":
            summary["skipped"] += 1
        elif body.get("created"):
            summary["created"] += 1
        elif body.get("status") == "NOT_AFFECTED" and cancelled_only:
            summary["cancelled"] += 1
        else:
            summary["updated"] += 1

    # Post-sync read-back for map decorations (lead ids + dismissed clients).
    if not nexy.dry_run:
        try:
            post = nexy.statuses(ext_ids)
            per_client: dict[str, list[dict]] = {}
            for ext, st in post.items():
                cid = ext.split("::", 1)[0]
                if st.get("exists"):
                    per_client.setdefault(cid, []).append(st)
            for cid, sts in per_client.items():
                open_lead = next((s for s in sts if s.get("status") not in ("NOT_AFFECTED", "EXPIRED") and not s.get("archived")), None)
                pick = open_lead or sts[0]
                if pick.get("leadId"):
                    client_lead[cid] = pick["leadId"]
                if sts and all(s.get("status") in DISMISSED_STATUSES for s in sts):
                    dismissed_clients.add(cid)
        except Exception as e:
            print(f"[nexy] post-sync status read failed: {e}", flush=True)

    summary["client_lead_ids"] = client_lead
    summary["not_affected_client_ids"] = sorted(dismissed_clients)

    view = {k: v for k, v in summary.items() if k != "client_lead_ids"}
    print(f"[nexy] summary: {view}", flush=True)
    print(f"[nexy] client_lead_ids resolved: {len(client_lead)}", flush=True)
    return summary
