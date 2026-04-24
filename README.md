# Jemena planned outages — auto-updating map

A self-updating interactive map showing Jemena's planned electricity outages
(Melbourne's north and west). A GitHub Action scrapes Jemena's website once a
day, matches each affected street to its OpenStreetMap geometry, rebuilds the
map, and publishes it to GitHub Pages.

The map URL stays the same forever; contents refresh every morning.

---

## First-time setup (once, ~15 minutes)

### 1. Create a GitHub account

Go to https://github.com/signup. Pick a username — it shows up in your public
URL later (e.g. `https://yourname.github.io/jemena-outages`), so choose
something work-appropriate.

### 2. Create the repository

- Click the `+` icon at the top-right → **New repository**.
- Repository name: `jemena-outages` (anything works, but short is nice).
- Visibility: **Public**. GitHub Pages requires public on free accounts.
- Tick **Add a README file**. You'll overwrite it in a moment.
- Click **Create repository**.

### 3. Upload the files

- On your new repo's home page, click **Add file → Upload files**.
- Drag every file from this folder into the browser — including the `.github`
  folder and the `docs` folder. GitHub preserves folder structure.
- Scroll down and click **Commit changes**.

### 4. Enable GitHub Pages

- Go to **Settings → Pages** (left sidebar).
- Under **Build and deployment**:
  - Source: **Deploy from a branch**
  - Branch: **main**, folder: **/docs**
- Click **Save**.
- Wait 1–2 minutes. The page will show your live URL, something like
  `https://yourname.github.io/jemena-outages/`.

### 5. First run

The workflow is scheduled for 6am Melbourne time daily, but you'll want to
check it works now rather than waiting until tomorrow.

- Go to the **Actions** tab.
- If GitHub asks whether to enable workflows on this repo, click **Enable**.
- Click **Update Jemena outages map** in the left sidebar.
- Click **Run workflow → Run workflow** (top-right green button).
- Watch the run. It takes about 3–5 minutes. Most time is spent installing
  Playwright's headless browser.

### 6. See the map

Visit your Pages URL (`https://yourname.github.io/jemena-outages/`). You
should see the map with today's outages.

### 7. Share the URL with your team

That's it — the same URL keeps showing fresh data every morning.

---

## How it works

Every day at 6am Melbourne time:

1. GitHub Actions spins up a Ubuntu VM.
2. `scrape.py` uses Playwright (headless Chrome) to open Jemena's planned
   outages page. The page is a JavaScript app, so a headless browser is
   needed — plain HTTP fetches get an empty shell.
3. The scraper reads the outage table from the rendered DOM.
4. For any suburb it hasn't seen before, it geocodes the name via
   OpenStreetMap's Nominatim service. Results cache in `.cache/suburbs.json`
   so we don't re-geocode day to day.
5. It builds one Overpass API query covering every affected street, gets the
   road geometries, and matches each segment to the Jemena outage data.
6. It renders `docs/index.html` using `template.html` and commits the result
   back to the repo. GitHub Pages auto-publishes the change.

If any step fails, the Action run goes red and GitHub emails you. The previous
day's map remains live until you fix it.

---

## Customising

### Change the run time

Edit `.github/workflows/update.yml`, line starting with `cron:`. The value is
UTC time:

- `0 20 * * *` = 06:00 AEST (standard time, May–Sep)
- `0 19 * * *` = 06:00 AEDT (daylight saving, Oct–Apr)

Pick whichever daylight regime matters more to you, or change it twice a year.
GitHub doesn't let cron know about timezones.

### Run more/less often

Same file, same line. Replace `0 20` with `0 */6` for every 6 hours, or
`0 20 * * 1-5` for weekdays only. Be polite to Jemena — once a day is
plenty for data they update in business hours.

### Different area (if Jemena ever change their scope)

`scrape.py` works suburb-by-suburb based on whatever's in the table. No
hardcoded suburb list; new suburbs get geocoded on the fly.

### Identify the bot

Set your repo URL in the User-Agent so Jemena can see who's hitting them if
they ever check logs. Open `scrape.py`, find the `REPO_URL` line, replace
with your URL. Or set a `REPO_URL` secret in the repo settings — it'll be
picked up via environment variable.

---

## When something breaks

### "Scrape failed — could not find outage table"

Jemena changed their page. The workflow saves two debug files to
`docs/_last_scrape.png` (screenshot) and `docs/_last_scrape.html` (raw HTML).
Open them to see what loaded. Most likely the table selector in
`scrape.py → scrape_outages()` needs adjusting — update the JavaScript inside
`page.evaluate(...)` to match the new markup.

### "Overpass failed"

The OpenStreetMap query server is volunteer-run and occasionally rate-limits.
The scraper tries three mirrors in sequence. If all three are down for 5+ hours
at a stretch, the daily run fails; tomorrow's run usually succeeds.

### "Nominatim failed"

Geocoding hiccups for a specific suburb. That suburb drops out of the map for
the day but the rest stays. If it keeps failing, check the name spelling in
the Jemena data.

### Action runs succeed but the Pages URL shows an old map

Wait 1–2 minutes after a successful run — GitHub Pages redeploys async. If
still stale after 5 minutes, go to Settings → Pages and click whatever "redeploy"
option is visible.

---

## Attribution and terms

This repo scrapes public data from jemena.com.au once a day and republishes
it in a different visual form. It's operational reference data, not
authoritative — always confirm directly with Jemena before acting on
anything critical (particularly life-support notifications).

Map data © OpenStreetMap contributors (ODbL). Base tiles © CARTO.
Outage data © Jemena.

---

## Running locally (if you want to test changes first)

    pip install -r requirements.txt
    python -m playwright install chromium
    python scrape.py

Output goes to `docs/index.html`, same as in CI. Open it in a browser to
verify before pushing.
