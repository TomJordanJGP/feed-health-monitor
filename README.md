# Feed Health Monitor

An employer-level early-warning view of JGP's job feeds. Every hour it records:
- whether each feed XML has been rebuilt recently (and how old it is);
- what changed since the feed's previous version: employers and vacancies added or removed, jobs expired as expected versus removed early;
- which vacancies aren't reaching Jobiqo;
- once a day, every HubSpot company and how it matches the feed employers (by Jobiqo ID, ATS ID and name), so a signed company whose jobs are in no feed shows up.

The data lands in BigQuery (`site-monitoring-421401.feed_health`), and a small Streamlit dashboard shows it. Slack alerts come later.

## Run it locally

```bash
python3 -m venv venv && venv/bin/pip install -r requirements-dev.txt
cp .env.example .env        # fill in FEED_MAP_URL and JOBIQO_EXPORT_URL
venv/bin/pytest
venv/bin/python -m feed_health.collect --dry-run
venv/bin/python -m feed_health.collect                        # writes to BQ_DATASET (feed_health_dev locally)
venv/bin/streamlit run dashboard/app.py --server.port 8503 --server.address localhost   # http://localhost:8503
```

The dashboard reads the production dataset (`feed_health`, written by the collector Action) unless
`FEED_HEALTH_DASHBOARD_DATASET=feed_health_dev` is set. `--server.address localhost` keeps a local run off the network.

Feed URLs and keys never go in the repo: locally they come from `.env` (see `.env.example`), in the
Action from repository secrets (`FEED_MAP_URL`, `JOBIQO_EXPORT_URL`, `FEED_HEALTH_COLLECTOR_KEY`,
`HUBSPOT_TOKEN`).

## HubSpot companies

The first collector run after 06:00 UTC each day pulls every HubSpot company (100 per request)
and replaces the `companies` table; `--force-hubspot` syncs on demand. Each company is matched to
the employers seen in the feeds three ways: Jobiqo ID (the feeds' `organization_id`, or Civil
Service's `jobiqo_id`), ATS ID (`old_account_id`) and tidied name. `match_status` is `agree`,
`conflict` (keys found different employers), `name_only` or `no_match`, and `duplicate_on` lists
keys another company shares. `v_companies` adds where each company's jobs are now: in a feed,
only on Jobiqo, last seen in a feed on a date, or never seen. A HubSpot failure never fails the
run: it's recorded on the `hubspot` row of `checks`, whose age is the time since the last
successful sync. The run log is public, so it prints counts only.

## Deploy on Streamlit Community Cloud

1. Give the app its own read-only service account: BigQuery Job User on the project, then
   `venv/bin/python -m feed_health.setup --admin-key <admin key> --dashboard-reader <its email>`
   for read access to `feed_health` only.
2. New app: this repo, branch `main`, main file `dashboard/app.py`, Python 3.13. Streamlit installs
   `dashboard/requirements.txt`.
3. In the app's Secrets, put that account's key under `[gcp_service_account]`, one `key = "value"` line per field
   of the JSON key file. The dashboard uses it in place of the local key file.
4. Choose who can view the app under Settings → Sharing.

## Adding or removing a feed

Edit `config/feeds.yml`. Feeds are keyed by their XML file name. Each has three flags:
- `collect`: fetch and store it.
- `show`: display it in the dashboard.
- `imported`: whether Jobiqo imports it; only imported feeds count in the all-feeds totals. A feed Jobiqo doesn't import (Tribepad) is still shown, and its jobs missing from Jobiqo count as "not imported" rather than "not reaching Jobiqo".

Every feed in `feed_map.xml` is checked. One without an entry is treated as collected, shown and imported, under its file name, with a "not in config/feeds.yml" note in the status view until it's added.

Staleness thresholds (orange / red hours) are set in `defaults` and can be overridden per feed.

## Layout

| Path | Role |
|---|---|
| `feed_health/feeds.py` | Conditional HTTP fetches, XML parsing, normalisers, content hash |
| `feed_health/logic.py` | Pure logic: version diff, gone-job classification, cross-feed matching, summaries |
| `feed_health/pipeline.py` | One run's results from the fetched feeds (pure, so tests replay it) |
| `feed_health/hubspot.py` | HubSpot company list (paged, with retries) |
| `feed_health/companies.py` | Pure matching of HubSpot companies to feed employers |
| `feed_health/store.py` | BigQuery tables, views, reads and load-job writes |
| `feed_health/collect.py` | The collector run (`python -m feed_health.collect`) |
| `feed_health/setup.py` | Creates the datasets, grants access, creates tables and views |
| `dashboard/` | Streamlit app (`app.py`) and its tested display logic (`helpers.py`) |
| `config/feeds.yml` | Feed membership and staleness thresholds |
| `.github/workflows/collect.yml` | The collector Action |
