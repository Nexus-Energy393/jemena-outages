"""
Second opinion on AusNet matches, from AusNet itself.

A boundary polygon says "somewhere in here". Two things AusNet publishes say
more, and until Nov 2026 the pipeline ignored both:

  1. details[] on every incident: the localities (with postcodes) that have at
     least one affected meter.
  2. The street search behind the outagetracker.com.au search box, which
     answers "is this street, in this suburb, in an outage" from the meter
     database.

Why it matters: on 25 Nov 2026 one Seaspray outage (843 customers) arrived
with a boundary stretching 193 km to Ringwood. Polygon-only matching put 283
watched sites "in zone" and opened 260 leads, plus ten Google Ads zones, for
suburbs that were never losing power. The town list alone clears every one.

Each AusNet record on an affected client gets one verdict:

  confirmed   AusNet's street search lists the site's street for this incident
  kept        inside the boundary and nothing contradicts it (town agrees, or
              the site has no usable address to check)
  demoted     inside the boundary and the town agrees, but AusNet does not
              list the street: stays as a "possible" at the bottom of the pile.
              Also: more street-confirmed sites than a point-only outage has
              customers, so no one of them can be called definite
  rejected    AusNet's own data says no: the town is not listed, or the site
              is beside the boundary / on a point-only outage and the street
              is not listed either

Rejected records are not deleted here. They are tagged `_rejected` and ride
along until the CRM sync has used them to close any lead a previous run opened
(nexy_leads.py), then scrape.py strips them before the map is built. A record
dropped only because it could not be checked (`_soft`: no usable address, or
the search was down) is left off this run's map and leads but never closes
anything: a lookup failure must not read as "not affected".
"""
from __future__ import annotations

import re

from ausnet import StreetSearch, clean_street, norm_town


def _site_town(client: dict) -> tuple[str, str]:
    return norm_town(client.get("suburb")), re.sub(r"\D", "", str(client.get("postcode") or ""))[:4]


def verify_ausnet_matches(affected: list[dict], outages: list[dict]) -> dict:
    """Mutates `affected` in place. Returns counts for the run log."""
    by_id = {o["incident_id"]: o for o in outages if o.get("network") == "Ausnet"}
    stats = {"confirmed": 0, "kept": 0, "demoted": 0, "rejected": 0, "lookups": 0, "lookup_errors": 0}
    if not by_id:
        return stats

    # Pass 1: the town gate (free), and collect the street questions worth asking.
    pending = []  # (affected row, record, was_definite, street, suburb)
    for a in affected:
        client = a["client"]
        site_town, site_pc = _site_town(client)
        for bucket, was_definite in (("definite", True), ("possible", False)):
            for rec in a.get(bucket, []):
                inc = by_id.get(rec.get("incident_id")) if rec.get("network") == "Ausnet" else None
                if not inc:
                    continue
                towns = inc.get("towns") or []
                names = {t["name"] for t in towns}
                postcodes = {t["postcode"] for t in towns if t["postcode"]}
                shaky = inc.get("polygon_degenerate") or inc.get("polygon_synthetic")
                town_known = bool(towns) and bool(site_town or site_pc)
                town_ok = town_known and (site_town in names or (site_pc and site_pc in postcodes))
                if town_known and not town_ok:
                    rec["_rejected"] = f"AusNet lists {inc.get('suburb') or 'other towns'} for this outage, not {client.get('suburb') or site_pc}"
                    continue
                if not town_known and inc.get("polygon_degenerate"):
                    rec["_rejected"] = "boundary is not usable and the site has no suburb or postcode to check"
                    rec["_soft"] = True
                    continue
                # Ask the street question only when the suburb is one AusNet
                # itself uses for this incident; a postcode-only town match
                # would be asking about the wrong locality.
                street = clean_street(client.get("address", "")) if site_town in names else ""
                if street:
                    pending.append((a, rec, was_definite, street, site_town))
                elif shaky:
                    # A circle around a pin or a clipped boundary is not
                    # evidence without the street.
                    rec["_rejected"] = "no street proof for a point-only or unusable boundary"
                    rec["_soft"] = True
                else:
                    rec["_verified"] = "town" if town_ok else "zone"
                    stats["kept"] += 1

    # Pass 2: street search, one request per distinct street + suburb.
    if pending:
        search = StreetSearch()
        answers = search.lookup_many((street, suburb) for _, _, _, street, suburb in pending)
        stats["lookups"], stats["lookup_errors"] = search.calls, search.errors
        for a, rec, was_definite, street, suburb in pending:
            incidents = answers.get(StreetSearch.key(street, suburb))
            inc = by_id[rec["incident_id"]]
            shaky = inc.get("polygon_degenerate") or inc.get("polygon_synthetic")
            if incidents is None:
                # Search unavailable: fall back to what the geometry said, but
                # never let shaky geometry stand on its own.
                if shaky:
                    rec["_rejected"] = "street search unavailable and the boundary alone is not enough"
                    rec["_soft"] = True
                else:
                    rec["_verified"] = "town"
                    stats["kept"] += 1
            elif rec["incident_id"] in incidents:
                rec["_verified"] = "street"
                rec["street"] = f"{street} (confirmed by AusNet)"
                if not was_definite:
                    rec["_promote"] = True
                stats["confirmed"] += 1
            elif was_definite and not shaky:
                rec["_verified"] = "street-not-listed"
                rec["_demote"] = True
                stats["demoted"] += 1
            else:
                rec["_rejected"] = f"AusNet does not list {street}, {suburb.title()} for this outage"

    # A point-only outage cannot make more sites "definite" than it has
    # customers. One HV customer on Burwood Highway, with seven watched shops
    # on that street inside the search circle (Westfield Knox, 2 Dec 2026), is
    # one site losing power, or one landlord's supply feeding all seven. We
    # cannot tell which from here, so each is shown as possible, not definite.
    shared: dict[str, list] = {}
    for a in affected:
        for bucket in ("definite", "possible"):
            for rec in a.get(bucket, []):
                inc = by_id.get(rec.get("incident_id")) or {}
                if rec.get("_verified") == "street" and inc.get("polygon_synthetic"):
                    shared.setdefault(rec["incident_id"], []).append((rec, bucket))
    for iid, recs in shared.items():
        customers = by_id[iid].get("customers") or 0
        if customers and len(recs) > customers:
            note = f"(on the outage's street; it affects {customers} customer{'' if customers == 1 else 's'})"
            for rec, bucket in recs:
                rec["_verified"] = "street-shared"
                rec["street"] = rec["street"].replace("(confirmed by AusNet)", note)
                rec.pop("_promote", None)
                if bucket == "definite":
                    rec["_demote"] = True
                stats["confirmed"] -= 1
                stats["demoted"] += 1

    # Apply promotions / demotions between the two buckets.
    by_incident: dict[str, tuple] = {}
    for a in affected:
        definite, possible = [], []
        for rec in a.get("definite", []):
            if rec.pop("_demote", False):
                possible.append({**rec, "_match": "near-zone", "_distance_m": 0})
            else:
                definite.append(rec)
        for rec in a.get("possible", []):
            if rec.pop("_promote", False):
                rec = {k: v for k, v in rec.items() if k != "_distance_m"}
                definite.append({**rec, "_match": "listed-street-edge"})
            else:
                possible.append(rec)
        a["definite"], a["possible"] = definite, possible
        for r in definite + possible:
            if r.get("_rejected"):
                stats["rejected"] += 1
                n, why, names = by_incident.get(r["incident_id"], (0, r["_rejected"], []))
                by_incident[r["incident_id"]] = (n + 1, why, names + [a["client"].get("name", "")])
    # One line per incident so a run log shows what was ruled out, and why.
    for iid, (n, why, names) in sorted(by_incident.items(), key=lambda kv: -kv[1][0])[:25]:
        sample = ", ".join(names[:3]) + (f" +{n - 3} more" if n > 3 else "")
        print(f"[verify] {iid}: ruled out {n} ({sample}): {why}", flush=True)
    return stats


def strip_rejected(affected: list[dict]) -> list[dict]:
    """Drop rejected records, and clients left with nothing. Call after the CRM sync."""
    kept = []
    for a in affected:
        a["definite"] = [r for r in a.get("definite", []) if not r.get("_rejected")]
        a["possible"] = [r for r in a.get("possible", []) if not r.get("_rejected")]
        if a["definite"] or a["possible"]:
            kept.append(a)
    return kept
