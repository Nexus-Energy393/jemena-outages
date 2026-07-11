# Jemena planned outages — auto-updating map + client impact

A self-updating interactive map showing Jemena's planned electricity outages
(Melbourne's north and west), with automatic identification of affected
client sites.

A GitHub Action scrapes Jemena's website once a day, matches each affected
street to its OpenStreetMap geometry, identifies which of your tracked
clients are in those areas, and publishes everything to GitHub Pages.

The map URL stays the same forever; contents refresh every morning.

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
- **Possibly affected**: they're within 200m of a shaded street segment

Both are shown separately so the team can prioritise. Definite goes red on
the map and gets a red chip on the affected page; possible goes amber.

The 200m default is set by `BUFFER_METRES` near the top of `scrape.py`.

---

## Operational stuff

### When the data changes
- Daily run at 06:00 AEST (07:00 AEDT) — see `.github/workflows/update.yml`.
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
python -m playwright install chromium
python scrape.py
```

Output goes to `docs/`. Open `docs/index.html` in a browser to verify.
