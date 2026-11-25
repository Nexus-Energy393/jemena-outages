# Planned outages map and lead feed (Jemena, AusNet, CitiPower, Powercor, United Energy)

A self-updating map of planned electricity outages across Victoria, matched
against the sites Nexus watches, published at outages.nexusenergy.au and pushed
into the Nexy CRM Leads inbox.

## Where the data comes from

Traced in Nov 2026 by reading what each distributor's own public outage pages
load. Nothing here is behind a login.

| Network | Forward view (days ahead) | On the day |
|---|---|---|
| Jemena | Planned-outages table: suburb, street, date, time, status (`jemena.com.au/api/ElectricityOutage/electricity-outage`). About 3 weeks ahead. | Outage-map feed adds the real boundary polygon, customers and an event id (read through the CRM relay, the feed is Australia-only). |
| AusNet | Outage Tracker combined list plus a boundary per incident, 5+ days ahead. Checked against AusNet's town list and street search, see below. | Same feed. |
| CitiPower, Powercor, United Energy | None published. Their feeds list an outage from the minute it starts. | Live feed with polygon, street, customers. |

So advance leads come from Jemena and AusNet. The other three sharpen the map
on the day but cannot warn anyone in advance.

## How a site is matched

- **Jemena, upcoming:** the site's street name and suburb appear in the table
  (definite), or the site is within 100 m of a listed street (possible, amber
  on the map; only raised as a lead within `NEXY_STREET_NEAR_M`, default 30 m).
- **Any network with a boundary:** the site is inside it (definite) or within
  100 m of its edge (possible).
- **AusNet, verified (`verify.py`):** a boundary match must also survive
  AusNet's own data. The site's suburb or postcode must be one AusNet lists
  for the incident, and where the site has a plain street address AusNet's
  street search is asked whether that street is in the outage. Confirmed
  streets are marked "confirmed"; a site AusNet's data rules out is dropped
  and any lead an earlier run opened for it is closed in the CRM.
- **AusNet outages with no boundary** (mostly 1 or 2 customers) only match a
  site when the street search confirms it.

`docs/health.json` records what each source returned on the last run. A source
that fails or comes back empty raises a warning on the Actions run.

---

## What's published

Every morning, three things land on the Pages site:

- **`/`** — the interactive map with shaded streets, suburb pins, and client
  markers (definitely affected in red, possibly affected in amber, others
  in grey if you toggle the layer on)
- **`/affected.html`** — a sortable, filterable table of impacted clients
  with their contact details. Linked from a button on the main map.
- **`/affected.csv`** — the same data as a spreadsheet for emails, mail
  merge, etc.

---

## The watch list — managed in the Nexy CRM

The tracked-client list lives in the **Nexy CRM**, not in this repo.
Each run fetches it from:

    GET https://crm.nexusenergy.au/api/intake/monitored-sites
    header: x-nexy-intake-secret: <NEXY_INTAKE_SECRET>

**To watch a client:** open their organisation in the CRM
(`crm.nexusenergy.au/organisations`) and switch on **Outage watch**. That
org's Sites — and its own head-office address — join the watch list on the
next run. Add a site or fix an address in the CRM and the map follows
automatically. There is no clients.csv to maintain any more.

How the feed maps to the map:

- Every watched org contributes each of its **Sites** (child organisations)
  that has an address or suburb, plus **itself** when it has a street
  address (single-site clients).
- `category` comes from the site's industry (falling back to the parent's) —
  it's the coloured chip on the map.
- The contact shown on the affected page is the site's first contact,
  falling back to the parent org's.
- Each row carries `crm_org_id`, which becomes the site's `client_id` — so
  CRM leads link straight back to the right organisation, permanently.

Resilience: the last good list is cached in `.cache/nexy_sites.csv`. If the
CRM is unreachable for a run, the cached list is used and the run continues.
A leftover `clients.csv` at the repo root is honoured only as a final
fallback (the historical list has been imported into the CRM).

Geocoding is unchanged: new addresses are geocoded once via OpenStreetMap
and cached in `.cache/clients.json`, so re-runs are instant.

In addition to the CRM watch list, the pipeline pulls major chains from
OpenStreetMap automatically (refreshed weekly): McDonald's, Hungry Jack's,
KFC, Aldi, Coles, Woolworths, IGA, Bunnings, Officeworks, Kmart, Target,
Big W, and shopping centres. Edit the `CHAINS` list in `scrape.py` to
add or remove.

---

## Affected logic

A client appears in the affected list if either:

- **Definitely affected**: their address's street name + suburb appears
  exactly in the day's outage list, OR
- **Possibly affected**: they're within 100m of a shaded street segment

Both are shown separately so the team can prioritise. Definite goes red on
the map and gets a red chip on the affected page; possible goes amber.

The 100m default is set by `BUFFER_METRES` near the top of `scrape.py`.

---

## Operational stuff

### When the data changes
- Hourly at 23 past, 7 am to 6 pm Melbourne time — see `.github/workflows/update.yml`.
- The chain list refreshes weekly to avoid hammering OpenStreetMap.
- Every run saves debug snapshots to `docs/_last_scrape.png` and
  `docs/_last_scrape_raw.json` so failures are diagnosable.

### When something breaks
- Failed run → check the Actions tab → click the red run → look at the
  failing step. Most likely cause is Jemena changing their page markup;
  the debug PNG and HTML get committed even on failure.
- CRM watch-list fetch failed → the run logs
  `[clients] CRM watch-list fetch failed` and continues on the cached list.
  Check `NEXY_INTAKE_SECRET` and that the CRM is up.
- Failed geocode for a specific client row → that client just doesn't
  appear on the map until you fix the address (in the CRM). No knock-on
  effect.
- Overpass down → all three mirrors are tried in sequence. If all fail,
  the workflow fails for the day, retries tomorrow.

### Cost
Free. GitHub Actions gives 2,000 free minutes/month for public repos
(this run uses ~5/day = ~150/month). GitHub Pages is free. OSM/Nominatim/
Overpass are free at our request rates. CARTO basemap tiles are free for
non-commercial use up to 75k tiles/month.

---

## Setup, first time

Same as before:

1. Create the repo, upload these files
2. Settings → Pages → deploy from `main` / `/docs`
3. Actions tab → enable workflows → run "Update Jemena outages map" once
4. Your live URL: `https://<username>.github.io/<repo-name>/`

If you're upgrading an existing v1 repo, you only need to:

1. Upload the new `scrape.py`
2. Upload the new `template.html`
3. Upload the new `affected_template.html`
4. Set the `NEXY_INTAKE_SECRET` repo secret (the watch list comes from the CRM)
5. Trigger a manual workflow run

Existing caches (`.cache/suburbs.json`) carry over. New caches
(`.cache/clients.json`, `.cache/chains.json`, `.cache/nexy_sites.csv`) get
created on first run.

---

## Nexy CRM leads sink

Every affected client that meets the duration threshold is pushed to the
**Nexy CRM Leads inbox** (`crm.nexusenergy.au/leads`) by `nexy_leads.py`
after each scrape. The sync is idempotent (one lead per client per outage,
keyed `<client_id>::<incident_id>`) and two-way: cancellations close their
leads automatically, and clients dismissed in the CRM (or via the table's
Hide button) are excluded from the map on the next run. The map and table
deep-link each client to its lead (`/leads?lead=<id>`), and each lead links
back with `?focus=<client_id>`.

For CRM-watched sites the `client_id` IS the CRM organisation id, so every
lead lands pre-linked to the right organisation (existing-client scoring,
site contacts and upsell context attach automatically).

Configuration (repo **Settings → Secrets and variables → Actions**):

| Name | Type | Purpose |
|---|---|---|
| `NEXY_INTAKE_SECRET` | secret (required) | The CRM's `INTAKE_WEBHOOK_SECRET` — used for both the leads sink and the watch-list fetch |
| `NEXY_DRY_RUN` | variable, default `false` | `true` = log without pushing |
| `NEXY_MIN_HOURS` | variable, default `6` | Minimum outage hours for a lead |
| `NEXY_MAX_DAYS_AHEAD` | variable, default `21` | Don't create leads for outages further out than this |
| `NEXY_SITES_URL` | variable, optional | Override the watch-list endpoint (defaults to `<CRM_BASE_URL>/api/intake/monitored-sites`) |

The table's Hide / Confirm / Archive buttons call the CRM's intake endpoint
directly from the browser and prompt once for the CRM's
`INTAKE_BROWSER_SECRET` (stored in localStorage).

---

## Attribution and disclaimer

Outage data © Jemena. Map tiles © CARTO and OpenStreetMap contributors
(ODbL). Chain data and street geometries © OpenStreetMap contributors.

This map is an unofficial visualisation. The "affected" determination is
a best-effort estimate based on street-name and proximity matching —
always confirm with Jemena and the customer directly before acting on
anything operationally critical. In particular, life-support customers
should rely on Jemena's own notifications, not this tool.

---

## Local testing

```
pip install -r requirements.txt
python -m unittest discover -s tests
NEXY_DRY_RUN=true python scrape.py
```

Output goes to `docs/`. Open `docs/index.html` in a browser to verify.
