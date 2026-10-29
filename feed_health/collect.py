"""One collector run: read state from BigQuery, fetch the feed map, every collected feed and
Jobiqo's export, compute the run (pipeline.compute_run), and write it back.

  venv/bin/python -m feed_health.collect                   # writes to BQ_DATASET
  venv/bin/python -m feed_health.collect --dry-run         # fetch and compute only
  venv/bin/python -m feed_health.collect --force-hubspot   # sync HubSpot even if done today

A problem with one feed is recorded against that feed and never stops the run; nor does a
HubSpot failure, which is recorded on the `hubspot` status row. Anything that stops the run
from being recorded (e.g. BigQuery unavailable) raises, so the Action goes red.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests

from google.cloud import bigquery

from feed_health import companies, feeds, hubspot, pipeline, store
from feed_health.config import Settings, load_settings
from feed_health.feeds import FeedParseError
from feed_health.pipeline import ALL_SCOPE, HUBSPOT_SCOPE, ExportObservation, Observation, RunOutput


def run(settings: Settings, dry_run: bool = False, force_hubspot: bool = False) -> RunOutput:
    started = time.monotonic()
    run_ts = datetime.now(timezone.utc).replace(microsecond=0)
    client = store.client_for(settings)
    states, prev_export_jobs, last_hubspot = store.read_state(client, settings)
    session = requests.Session()

    # Feed map -> which feeds exist. A configured feed missing from the map (or a map that
    # can't be read) is fetched from the map's folder, and flagged, rather than going dark.
    map_fetch = feeds.fetch(settings.feed_map_url, session=session)
    map_error, entries = map_fetch.error, []
    if map_fetch.status == 200:
        try:
            entries = feeds.parse_feed_map(map_fetch.body)
        except FeedParseError as exc:
            map_error = f"feed map: {exc}"
    urls = {e.file: e.url for e in entries}
    notes: dict[str, str] = {}
    for file, cfg in settings.feeds.items():
        if cfg.collect and file not in urls:
            urls[file] = urljoin(settings.feed_map_url, file)
            notes[file] = "feed map unavailable; fetched directly" if map_error else "not listed in feed_map.xml"

    observations = []
    for file, url in urls.items():
        cfg = settings.feed(file)
        if not cfg.collect:
            continue
        st = states.get(file)
        # Only ask "has it changed?" when there is a committed version to compare against.
        etag = st.last_check.etag if st and st.latest and st.last_check else None
        fetched = feeds.fetch(url, etag=etag, session=session)
        parsed, error = None, None
        if fetched.status == 200:
            try:
                parsed = feeds.parse_feed(fetched.body, run_ts)
            except FeedParseError as exc:
                error = str(exc)
        observations.append(Observation(file, cfg, fetched, parsed, error, notes.get(file)))

    export_fetch = feeds.fetch(settings.jobiqo_export_url, session=session)
    export, export_error = None, export_fetch.error
    if export_fetch.status == 200:
        try:
            export = feeds.parse_jobiqo_export(export_fetch.body)
        except FeedParseError as exc:
            export_error = f"export: {exc}"

    out = pipeline.compute_run(run_ts, settings, states, observations,
                               ExportObservation(export_fetch, export, export_error),
                               map_fetch, map_error, prev_export_jobs)
    hubspot_row, out.companies = sync_hubspot(client, settings, out, run_ts, last_hubspot, session, force_hubspot)
    out.checks.append(hubspot_row)
    all_row = next(r for r in out.checks if r["scope"] == ALL_SCOPE)
    all_row["run_seconds"] = round(time.monotonic() - started, 2)
    if not dry_run:
        store.write_run(client, settings, out, states)
    return out


def sync_hubspot(client: bigquery.Client, settings: Settings, out: RunOutput, run_ts: datetime,
                 last_row: dict | None, session: requests.Session, force: bool) -> tuple[dict, list[dict]]:
    """Once a day (or when forced): pull every HubSpot company and match it to the employers seen
    in the feeds. Returns the `hubspot` status row and the `companies` rows ([] if no sync)."""
    if not (force or pipeline.hubspot_due(run_ts, last_row["build_ts"] if last_row else None)):
        return pipeline.hubspot_row(run_ts, last_row), []
    if not settings.hubspot_token:
        return pipeline.hubspot_row(run_ts, last_row, error="HUBSPOT_TOKEN is not set"), []
    if not out.export_usable:  # companies are matched to the export too; retry on the next run
        return pipeline.hubspot_row(run_ts, last_row, note="waiting for a usable Jobiqo export"), []
    try:
        fetched = hubspot.fetch_companies(settings.hubspot_token, session)
    except hubspot.HubSpotError as exc:
        return pipeline.hubspot_row(run_ts, last_row, error=str(exc)), []
    employers = companies.merge_employers(
        store.read_employers(client, settings),
        ((e.org_key, e.org_name) for e in out.employer_rows),
        ((j.org_key, j.ats_org_id) for v in out.latest.values() for j in v.jobs))
    rows = companies.match_companies(fetched, employers, out.export_orgs, run_ts)
    return pipeline.hubspot_row(run_ts, last_row, synced=len(rows), note="synced this run"), rows


def report(out: RunOutput, settings: Settings, dry_run: bool, seconds: float) -> str:
    lines = [f"{'DRY RUN - nothing written' if dry_run else 'written to ' + settings.bq_dataset}"
             f" in {seconds:.1f}s"]
    for r in out.checks:
        if r["scope"] in pipeline.NON_FEED_SCOPES:
            continue
        age = f"{r['age_hours']:5.1f}h" if r["age_hours"] is not None else "   n/a"
        state = r["error"] or ("new version" if r["content_changed"] else
                               "rebuilt, same content" if r["rebuilt"] else "unchanged")
        lines.append(f"  {r['scope']:42s} HTTP {r['http_status'] or '---'}  age {age}  jobs {r['jobs'] or 0:5d}  "
                     f"{state}{'  [' + r['note'] + ']' if r['note'] else ''}")
    export_row = next(r for r in out.checks if r["scope"] == pipeline.EXPORT_SCOPE)
    total = next(r for r in out.checks if r["scope"] == ALL_SCOPE)
    lines.append(f"  jobiqo export: {export_row['export_status']} ({export_row['jobs'] or 0} jobs)")
    lines.append(f"  all feeds: {total['jobs']} vacancies, {total['employers']} employers, "
                 f"not reaching Jobiqo: {total['not_reaching']}, errors: {total['errors']}")
    # HubSpot company names are internal and this log is public: counts only.
    hs = next(r for r in out.checks if r["scope"] == HUBSPOT_SCOPE)
    if out.companies:
        lines.append("  hubspot: synced " + ", ".join(f"{k} {v}" for k, v in companies.summary(out.companies).items()))
    else:
        last = hs["build_ts"].strftime("%Y-%m-%d %H:%M UTC") if hs["build_ts"] else "never"
        lines.append(f"  hubspot: {hs['error'] or hs['note'] or 'not due'} (last synced {last})")
    flagged = [e for e in out.employer_rows if e.removed_unexpected or (e.jobs_prev and not e.still_showing)]
    for e in flagged:
        lines.append(f"  ! {e.source}: {e.org_name} {e.jobs_prev} -> {e.jobs_now} "
                     f"({e.removed_unexpected} removed before their closing date)")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the feed-health collector once.")
    parser.add_argument("--dry-run", action="store_true", help="fetch and compute, but write nothing")
    parser.add_argument("--force-hubspot", action="store_true", help="sync HubSpot companies even if done today")
    args = parser.parse_args()
    settings = load_settings()
    started = time.monotonic()
    out = run(settings, dry_run=args.dry_run, force_hubspot=args.force_hubspot)
    print(report(out, settings, args.dry_run, time.monotonic() - started))
    return 0


if __name__ == "__main__":
    sys.exit(main())
