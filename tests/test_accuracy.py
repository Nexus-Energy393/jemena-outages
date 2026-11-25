"""Run with: python -m unittest discover -s tests -v   (no network needed)"""
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import ausnet  # noqa: E402
import nexy_leads  # noqa: E402
import scrape  # noqa: E402
import verify  # noqa: E402

SEASPRAY_TOWNS = [{"name": "SEASPRAY", "postcode": "3851", "customers": 428},
                  {"name": "NORTHCOTE", "postcode": "3070", "customers": 1}]


def incident(iid="INCD-1", towns=None, degenerate=False, synthetic=False):
    return {"incident_id": iid, "network": "Ausnet", "towns": towns if towns is not None else SEASPRAY_TOWNS,
            "suburb": "Seaspray", "polygon_degenerate": degenerate, "polygon_synthetic": synthetic}


def site(name, address, suburb, postcode="", bucket="definite", iid="INCD-1"):
    rec = {"network": "Ausnet", "incident_id": iid, "_match": "in-zone" if bucket == "definite" else "near-zone"}
    if bucket == "possible":
        rec["_distance_m"] = 40
    row = {"client": {"name": name, "address": address, "suburb": suburb, "postcode": postcode}, "definite": [], "possible": []}
    row[bucket].append(rec)
    return row


class FakeSearch:
    answers: dict = {}
    def __init__(self): self.calls = 0; self.errors = 0
    key = staticmethod(ausnet.StreetSearch.key)
    def lookup_many(self, pairs):
        out = {}
        for street, suburb in pairs:
            self.calls += 1
            out[self.key(street, suburb)] = FakeSearch.answers.get(self.key(street, suburb), [])
        return out


class VerifyTests(unittest.TestCase):
    def run_verify(self, rows, incidents, answers=None):
        FakeSearch.answers = answers or {}
        with mock.patch.object(verify, "StreetSearch", FakeSearch):
            return verify.verify_ausnet_matches(rows, incidents)

    def test_site_in_boundary_but_not_in_listed_towns_is_rejected(self):
        # The 25 Nov 2026 failure: a Traralgon site inside a Seaspray outage's sliver.
        rows = [site("ALDI Traralgon", "141 Argyle Street", "Traralgon", "3844")]
        self.run_verify(rows, [incident(degenerate=True)])
        rec = rows[0]["definite"][0]
        self.assertIn("_rejected", rec)
        self.assertNotIn("_soft", rec)  # firm: closes the stale lead
        self.assertEqual(verify.strip_rejected(rows), [])

    def test_street_confirmed_by_ausnet_is_definite(self):
        rows = [site("Servo", "12 Main Road", "Seaspray", "3851")]
        stats = self.run_verify(rows, [incident()], {"MAIN ROAD|SEASPRAY": ["INCD-1"]})
        self.assertEqual(rows[0]["definite"][0]["_verified"], "street")
        self.assertEqual(stats["confirmed"], 1)

    def test_in_zone_but_street_not_listed_is_demoted_not_dropped(self):
        rows = [site("Servo", "12 Main Road", "Seaspray", "3851")]
        self.run_verify(rows, [incident()], {"MAIN ROAD|SEASPRAY": ["INCD-OTHER"]})
        self.assertEqual(rows[0]["definite"], [])
        self.assertEqual(rows[0]["possible"][0]["_verified"], "street-not-listed")

    def test_near_miss_with_confirmed_street_is_promoted(self):
        rows = [site("Servo", "12 Main Road", "Seaspray", "3851", bucket="possible")]
        self.run_verify(rows, [incident()], {"MAIN ROAD|SEASPRAY": ["INCD-1"]})
        self.assertEqual(rows[0]["possible"], [])
        self.assertEqual(rows[0]["definite"][0]["_match"], "listed-street-edge")

    def test_postcode_match_passes_the_town_gate(self):
        rows = [site("Shop", "Shop 2, Plaza", "The Honeysuckles", "3851")]
        self.run_verify(rows, [incident()])
        self.assertEqual(rows[0]["definite"][0]["_verified"], "town")

    def test_point_only_outage_needs_street_proof_and_never_closes_a_lead_without_it(self):
        rows = [site("Shop", "Cnr High St & Low St", "Seaspray", "3851")]
        self.run_verify(rows, [incident(synthetic=True)])
        rec = rows[0]["definite"][0]
        self.assertTrue(rec.get("_rejected") and rec.get("_soft"))

    def test_point_only_outage_with_one_customer_cannot_make_several_sites_definite(self):
        # Westfield Knox, 2 Dec 2026: 1 HV customer, seven shops on the street.
        rows = [site(n, "425 Burwood Highway", "Seaspray", "3851") for n in ("Woolworths", "ALDI", "Subway")]
        inc = {**incident(synthetic=True), "customers": 1}
        self.run_verify(rows, [inc], {"BURWOOD HIGHWAY|SEASPRAY": ["INCD-1"]})
        for r in rows:
            self.assertEqual(r["definite"], [])
            self.assertEqual(r["possible"][0]["_verified"], "street-shared")
        # ...but a single matching site on a one-customer outage stays definite
        one = [site("Factory", "12 Main Road", "Seaspray", "3851")]
        self.run_verify(one, [inc], {"MAIN ROAD|SEASPRAY": ["INCD-1"]})
        self.assertEqual(one[0]["definite"][0]["_verified"], "street")

    def test_search_failure_is_unknown_not_a_no(self):
        rows = [site("Servo", "12 Main Road", "Seaspray", "3851")]
        self.run_verify(rows, [incident()], {"MAIN ROAD|SEASPRAY": None})
        self.assertEqual(rows[0]["definite"][0]["_verified"], "town")


class AusnetHelperTests(unittest.TestCase):
    def test_clean_street(self):
        self.assertEqual(ausnet.clean_street("94/72-82 Princes Drive"), "Princes Drive")
        self.assertEqual(ausnet.clean_street("245-249 Church Street, Cowes"), "Church Street")
        self.assertEqual(ausnet.clean_street("Cnr Maroondah Highway, New St"), "")
        self.assertEqual(ausnet.clean_street("32 Broadway"), "")  # one word would prefix-match too much

    def test_degenerate_boundary_is_clipped_to_the_incident(self):
        sliver = [[-37.81, 145.21], [-38.207, 147.405], [-38.35, 147.22], [-38.44, 147.09], [-38.47, 147.01]]
        display, degenerate = ausnet.repair_polygon(sliver, -38.188, 147.092)
        self.assertTrue(degenerate)
        self.assertTrue(all(p[1] > 146.9 for p in display))
        sane = [[-38.45, 145.23], [-38.45, 145.25], [-38.46, 145.24]]
        self.assertEqual(ausnet.repair_polygon(sane, -38.45, 145.24), (sane, False))

    def test_towns_come_from_details_biggest_first(self):
        towns = ausnet._row_towns({"details": [{"townName": "NORTHCOTE", "postCode": 3070, "customerCount": 1},
                                               {"townName": "Mt. Eliza", "postCode": 3930, "customerCount": 40}]})
        self.assertEqual([t["name"] for t in towns], ["MOUNT ELIZA", "NORTHCOTE"])
        self.assertEqual(ausnet.town_label(towns), "Mount Eliza, Northcote")


class JemenaTableTests(unittest.TestCase):
    NOW = datetime(2026, 11, 25, 12, 0)

    def parse(self, rows):
        with mock.patch.object(scrape, "melbourne_now", return_value=self.NOW):
            return scrape._parse_planned_table(rows, now=self.NOW)

    def test_rows_become_upcoming_street_entries(self):
        out = self.parse([{"Suburb": "ASCOT VALE", "Street": "MT ALEXANDER RD", "Date": "04-Dec", "Day": "Friday", "Time": "0830-1530", "Status": "Scheduled"}])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["street"], "Mt Alexander Road")
        self.assertEqual(out[0]["start_dt"], datetime(2026, 12, 4, 8, 30))
        self.assertEqual((out[0]["end_dt"] - out[0]["start_dt"]).total_seconds() / 3600, 7.0)

    def test_finished_and_unreadable_rows_are_skipped_and_overnight_rolls_over(self):
        out = self.parse([
            {"Suburb": "PRESTON", "Street": "BELL ST", "Date": "24-Nov", "Time": "0730-1600", "Status": "Scheduled"},
            {"Suburb": "PRESTON", "Street": "BELL ST", "Date": "soon", "Time": "0730-1600", "Status": "Scheduled"},
            {"Suburb": "AVONDALE HEIGHTS", "Street": "MILITARY RD", "Date": "26-Nov", "Time": "1900-0400", "Status": None},
        ])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["end_dt"], datetime(2026, 11, 27, 4, 0))
        self.assertEqual(out[0]["status"], "Scheduled")

    def test_mt_and_mount_are_one_street(self):
        self.assertEqual(scrape.norm_key("MT ALEXANDER ROAD"), scrape.norm_key("Mount Alexander Road"))


class LeadSyncTests(unittest.TestCase):
    def opps(self, definite=(), possible=()):
        affected = [{"client": {"client_id": "org1", "name": "Coles Preston", "suburb": "Preston"},
                     "definite": list(definite), "possible": list(possible)}]
        return nexy_leads.prepare_opportunities(affected, 6.0, None)

    BASE = {"suburb": "Preston", "street": "Bell Street", "start": "Tue 1 Dec, 7:30 AM", "end": "4:00 PM",
            "start_iso": "2026-12-01T07:30:00", "end_iso": "2026-12-01T16:00:00", "status": "Scheduled", "duration_hours": 8.5}

    def test_ruled_out_match_becomes_a_retraction(self):
        o = self.opps(definite=[{**self.BASE, "network": "Ausnet", "incident_id": "INCD-9", "_match": "in-zone", "_rejected": "not in listed towns"}])
        self.assertEqual(len(o), 1)
        self.assertTrue(o[0]["_retraction"])
        self.assertIn("cancel", o[0]["outageStatus"].lower())  # the CRM's close trigger

    def test_unverifiable_match_is_left_alone(self):
        self.assertEqual(self.opps(definite=[{**self.BASE, "network": "Ausnet", "incident_id": "INCD-9", "_rejected": "x", "_soft": True}]), [])

    def test_weak_near_street_match_is_map_only_but_a_close_one_is_a_lead(self):
        self.assertEqual(self.opps(possible=[{**self.BASE, "_distance_m": 80}]), [])
        self.assertEqual(len(self.opps(possible=[{**self.BASE, "_distance_m": 12}])), 1)
        # polygon near-zone matches are not street guesses and are unaffected
        self.assertEqual(len(self.opps(possible=[{**self.BASE, "_match": "near-zone", "_distance_m": 80, "incident_id": "X"}])), 1)


if __name__ == "__main__":
    unittest.main()
