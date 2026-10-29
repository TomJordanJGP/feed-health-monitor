"""One collector run as a pure function: given what was fetched this run and what was known
before it, work out the new versions, employer checks, Jobiqo gaps and per-scope check rows.

`collect.py` wraps this with the I/O (BigQuery reads, HTTP fetches, BigQuery writes). The
replay regression test drives the same function with Jobiqo's historic import files, so the
logic that runs hourly is the logic that was validated.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta

from feed_health import logic
from feed_health.config import FeedConfig, Settings
from feed_health.feeds import FetchResult, JobiqoExport, ParsedFeed
from feed_health.logic import Clusters, DiffContext, EmployerCheck, Gap, Version

FEED_MAP_SCOPE = "feed_map"
EXPORT_SCOPE = "jobiqo_export"
ALL_SCOPE = "ALL"
HUBSPOT_SCOPE = "hubspot"
NON_FEED_SCOPES = frozenset({FEED_MAP_SCOPE, EXPORT_SCOPE, ALL_SCOPE, HUBSPOT_SCOPE})

# An export organisation is linked to a feed employer when this share of its feed jobs agree
# (the rule used for the 29 Oct HubSpot companies x jobs export).
EXPORT_LINK_MIN_SHARE = 0.6

# HubSpot companies are synced once a day, on the first run after this hour (UTC). The status
# row turns orange once a day's sync has been missed, red after two.
HUBSPOT_SYNC_HOUR = 6
HUBSPOT_ORANGE_HOURS, HUBSPOT_RED_HOURS = 30.0, 54.0


@dataclass(frozen=True)
class LastCheck:
    """A source's row from the previous run, carried forward when nothing new was fetched."""
    etag: str | None
    generation: str | None
    last_modified: datetime | None
    build_ts: datetime | None
    last_build_date: datetime | None
    duplicate_ids: int | None
    jobs: int | None


@dataclass(frozen=True)
class SourceState:
    latest: Version | None = None
    latest_hash: str | None = None
    previous: Version | None = None
    last_check: LastCheck | None = None


@dataclass(frozen=True)
class Observation:
    """What this run saw for one source. `parsed` is set only on a 200 that parsed cleanly."""
    source: str
    cfg: FeedConfig
    fetch: FetchResult | None
    parsed: ParsedFeed | None = None
    error: str | None = None
    note: str | None = None          # a warning that doesn't stop processing


@dataclass(frozen=True)
class ExportObservation:
    fetch: FetchResult | None
    export: JobiqoExport | None
    error: str | None


@dataclass
class RunOutput:
    new_versions: dict[str, Version]
    new_version_meta: dict[str, dict]
    employer_rows: list[EmployerCheck]
    gaps: list[Gap]
    checks: list[dict]
    latest: dict[str, Version]
    previous: dict[str, Version | None]
    export_usable: bool
    employer_live: list[dict]      # per (source, employer): listings published on Jobiqo this run
    duplicates: list[dict]         # one row per copy of a vacancy listed in 2+ shown, imported feeds
    export_orgs: list[dict]        # the export's employers, linked to feed employers (usable export only)
    companies: list[dict] = field(default_factory=list)   # HubSpot companies, set by collect on a sync run


def build_time(fetch: FetchResult, parsed: ParsedFeed, run_ts: datetime) -> datetime:
    """When the XML was built: the storage write time (GCS Last-Modified). It matches
    <lastBuildDate> to the second today, and unlike lastBuildDate it can't be copied forward
    by a job that re-uploads an old file."""
    return fetch.last_modified or parsed.build_date or run_ts


def version_id(source: str, fetch: FetchResult, built: datetime) -> str:
    """Deterministic per observed file, so a crashed run's orphans are found and replaced."""
    return f"{source}@{fetch.generation or built.strftime('%Y%m%dT%H%M%SZ')}"


def compute_run(run_ts: datetime, settings: Settings, states: dict[str, SourceState],
                observations: list[Observation], export_obs: ExportObservation | None,
                feed_map_fetch: FetchResult | None = None, feed_map_error: str | None = None,
                prev_export_jobs: int | None = None) -> RunOutput:
    mon = settings.monitor
    obs_by_source = {o.source: o for o in observations}

    # 1. New versions: parsed files whose content differs from the source's latest version.
    new: dict[str, Version] = {}
    meta: dict[str, dict] = {}
    for o in observations:
        if o.parsed is None or o.fetch is None:
            continue
        st = states.get(o.source, SourceState())
        if st.latest is not None and st.latest_hash == o.parsed.content_hash:
            continue  # rebuilt with identical content: build time moves on, the version doesn't
        built = build_time(o.fetch, o.parsed, run_ts)
        vid = version_id(o.source, o.fetch, built)
        new[o.source] = Version(o.source, vid, built, o.parsed.jobs)
        meta[o.source] = {"content_hash": o.parsed.content_hash, "generation": o.fetch.generation,
                          "last_build_date": o.parsed.build_date, "first_seen_ts": run_ts}

    latest: dict[str, Version] = {}
    previous: dict[str, Version | None] = {}
    for source in obs_by_source:
        st = states.get(source, SourceState())
        if source in new:
            latest[source], previous[source] = new[source], st.latest
        elif st.latest is not None:
            latest[source], previous[source] = st.latest, st.previous

    # 2. Jobiqo export, and whether it can be trusted this run.
    export = export_obs.export if export_obs else None
    export_status = export_obs.error if export_obs and export_obs.error else None
    if export is not None and export_status is None:
        export_status = logic.export_problem(
            export.jobs, prev_export_jobs, export_obs.fetch.last_modified if export_obs.fetch else None,
            run_ts, mon.export_max_shrink, timedelta(hours=mon.export_max_age_hours))
    export_usable = export is not None and export_status is None

    # 3. Names (Civil Service needs the export), first-seen carry-forward, then clusters.
    names = logic.employer_names([*latest.values(), *(v for v in previous.values() if v)], export)
    matched_new: dict[str, dict] = {}
    for source, curr in new.items():
        prev = states.get(source, SourceState()).latest
        matched = logic.match_versions(prev, curr) if prev else {}
        matched_new[source] = matched
        first = logic.carry_first_seen(prev, curr.jobs, curr.build_ts, matched)
        curr = logic.with_names(replace(curr, first_seen=first), names)
        new[source] = latest[source] = curr

    in_play = {v.version_id: v for v in [*latest.values(), *(p for p in previous.values() if p)]}
    clusters = Clusters(in_play.values())
    imported = frozenset(s for s in latest if obs_by_source[s].cfg.imported)
    ctx = DiffContext(clusters, {s: v.version_id for s, v in latest.items()}, imported,
                      timedelta(hours=mon.expiry_grace_hours))

    # 4. Employer checks for each new version.
    employer_rows: list[EmployerCheck] = []
    for source, curr in new.items():
        prev = states.get(source, SourceState()).latest
        employer_rows.extend(logic.employer_checks(prev, curr, matched_new[source], ctx, names))

    # 5. Not reaching Jobiqo (only with a trustworthy export).
    gaps: list[Gap] = []
    if export_usable:
        gaps = logic.not_reaching(latest.values(), clusters, export.ids, run_ts,
                                  timedelta(hours=mon.not_reaching_grace_hours), names)
    gaps_by_source: dict[str, int] = {}
    for g in gaps:
        gaps_by_source[g.source] = gaps_by_source.get(g.source, 0) + 1

    # 6. What's live on Jobiqo per feed and employer, and vacancies listed in two feeds.
    totals = [s for s in latest if obs_by_source[s].cfg.show and obs_by_source[s].cfg.imported]
    live = logic.live_listings(latest.values(), export.ids) if export_usable else None
    groups = logic.duplicate_groups([latest[s] for s in totals], clusters,
                                    export.ids if export_usable else None, names)
    employer_live = [{"check_ts": run_ts, "source": s, "org_key": org, "live": n}
                     for (s, org), n in sorted(live.by_employer.items())] if live else []
    links = logic.link_export_orgs(latest.values(), export, EXPORT_LINK_MIN_SHARE) if export_usable else {}
    export_orgs = [{"check_ts": run_ts, "export_org_id": o.org_id, "company": o.company, "jobs": o.jobs,
                    "site_jobs": o.site_jobs, "org_key": links.get(o.org_id, (None, None))[0],
                    "link_share": links.get(o.org_id, (None, None))[1]}
                   for o in sorted(export.orgs.values(), key=lambda o: o.org_id)] if export_usable else []
    duplicate_rows = [{"check_ts": run_ts, "group_id": g.group_id, "basis": g.basis, "source": m.source,
                       "job_id": m.job_id, "org_key": m.org_key, "org_name": m.org_name, "title": m.title,
                       "live": m.live} for g in groups for m in g.members]

    # 7. Check rows: one per source, the feed map, the Jobiqo export, and the all-feeds total.
    checks: list[dict] = []
    for o in observations:
        row = _source_row(o, states.get(o.source, SourceState()), latest.get(o.source),
                          previous.get(o.source), o.source in new, run_ts, export_usable,
                          gaps_by_source.get(o.source, 0))
        row["live"] = live.by_source.get(o.source) if live and o.source in latest else None
        checks.append(row)

    checks.append(_plain_row(FEED_MAP_SCOPE, "feed_map.xml", run_ts, feed_map_fetch, feed_map_error))
    export_row = _plain_row(EXPORT_SCOPE, "Jobiqo export (Appcast)", run_ts,
                            export_obs.fetch if export_obs else None, export_obs.error if export_obs else "not fetched")
    export_row["jobs"] = export.jobs if export else None
    export_row["employers"] = export.organizations if export else None
    export_row["export_status"] = "ok" if export_usable else (export_status or "unavailable")
    export_row["orange_hours"], export_row["red_hours"] = mon.export_max_age_hours / 2, mon.export_max_age_hours
    checks.append(export_row)

    total = logic.all_feeds_summary([latest[s] for s in totals],
                                    [previous[s] or latest[s] for s in totals], clusters, names)
    all_row = _empty_row(ALL_SCOPE, "All feeds", run_ts)
    all_row.update(_summary_fields(total))
    all_row["jobs"], all_row["employers"] = total.vacancies_total, total.employers_total
    all_row["not_reaching"] = logic.distinct_gaps((g for g in gaps if g.source in totals), clusters) if export_usable else None
    all_row["export_status"] = export_row["export_status"]
    all_row["errors"] = sum(1 for r in checks if r["error"])
    all_row["cluster_conflicts"] = clusters.same_version_conflicts()
    all_row["live"] = sum(live.by_source.get(s, 0) for s in totals) if live else None
    all_row["duplicates"] = len(groups)
    checks.append(all_row)

    return RunOutput(new_versions=new, new_version_meta=meta, employer_rows=employer_rows, gaps=gaps,
                     checks=checks, latest=latest, previous=previous, export_usable=export_usable,
                     employer_live=employer_live, duplicates=duplicate_rows, export_orgs=export_orgs)


# --------------------------------------------------------------------------- check rows

CHECK_FIELDS = (
    "check_ts", "scope", "label", "configured", "collect", "show", "imported",
    "http_status", "error", "note", "etag", "generation", "last_modified", "build_ts", "last_build_date",
    "age_hours", "orange_hours", "red_hours", "rebuilt", "content_changed", "version_id",
    "jobs", "employers", "duplicate_ids",
    "employers_added", "employers_removed", "vacancies_added", "vacancies_removed",
    "not_reaching", "export_status", "errors", "cluster_conflicts", "run_seconds",
    "live", "duplicates",   # added 2026-09-29; appended so the table migration only adds columns
)


def _empty_row(scope: str, label: str, run_ts: datetime) -> dict:
    row = dict.fromkeys(CHECK_FIELDS)
    row.update(check_ts=run_ts, scope=scope, label=label)
    return row


def _summary_fields(s: logic.Summary) -> dict:
    return {"employers_added": s.employers_added, "employers_removed": s.employers_removed,
            "vacancies_added": s.vacancies_added, "vacancies_removed": s.vacancies_removed}


def _age(run_ts: datetime, built: datetime | None) -> float | None:
    return round((run_ts - built).total_seconds() / 3600, 3) if built else None


def _plain_row(scope: str, label: str, run_ts: datetime, fetch: FetchResult | None, error: str | None) -> dict:
    row = _empty_row(scope, label, run_ts)
    if fetch is not None:
        row.update(http_status=fetch.status, etag=fetch.etag, generation=fetch.generation,
                   last_modified=fetch.last_modified, build_ts=fetch.last_modified,
                   age_hours=_age(run_ts, fetch.last_modified))
    row["error"] = error or (fetch.error if fetch else None)
    return row


def hubspot_due(run_ts: datetime, last_success: datetime | None) -> bool:
    """Once a day: due when the last successful sync is older than the latest 06:00 UTC."""
    boundary = run_ts.replace(hour=HUBSPOT_SYNC_HOUR, minute=0, second=0, microsecond=0)
    if run_ts < boundary:
        boundary -= timedelta(days=1)
    return last_success is None or last_success < boundary


def hubspot_row(run_ts: datetime, previous: dict | None, synced: int | None = None,
                error: str | None = None, note: str | None = None) -> dict:
    """The HubSpot sync's status row. build_ts is the last successful sync, so its age (and
    colour) keeps growing while syncs fail; employers is the number of companies it wrote. A run
    that didn't try carries the previous outcome forward, error included."""
    prev = previous or {}
    row = _empty_row(HUBSPOT_SCOPE, "HubSpot companies", run_ts)
    row.update(orange_hours=HUBSPOT_ORANGE_HOURS, red_hours=HUBSPOT_RED_HOURS, note=note)
    if synced is not None:
        row.update(build_ts=run_ts, employers=synced)
    else:
        row.update(build_ts=prev.get("build_ts"), employers=prev.get("employers"),
                   error=error if error is not None else prev.get("error"))
    row["age_hours"] = _age(run_ts, row["build_ts"])
    return row


def _source_row(o: Observation, st: SourceState, latest: Version | None, previous: Version | None,
                changed: bool, run_ts: datetime, export_usable: bool, gap_count: int) -> dict:
    cfg, last = o.cfg, st.last_check
    row = _empty_row(o.source, cfg.label, run_ts)
    row.update(configured=cfg.configured, collect=cfg.collect, show=cfg.show, imported=cfg.imported,
               orange_hours=cfg.orange_hours, red_hours=cfg.red_hours, note=o.note)
    fetched_ok = o.parsed is not None and o.fetch is not None and o.fetch.status == 200
    if o.fetch is not None:
        row["http_status"] = o.fetch.status
    if fetched_ok:
        built = build_time(o.fetch, o.parsed, run_ts)
        row.update(etag=o.fetch.etag, generation=o.fetch.generation, last_modified=o.fetch.last_modified,
                   build_ts=built, last_build_date=o.parsed.build_date, duplicate_ids=o.parsed.duplicate_ids,
                   rebuilt=last is None or o.fetch.generation != last.generation)
    elif last is not None:  # 304, or an error: carry the last known build forward so its age keeps growing
        row.update(etag=last.etag, generation=last.generation, last_modified=last.last_modified,
                   build_ts=last.build_ts, last_build_date=last.last_build_date,
                   duplicate_ids=last.duplicate_ids, rebuilt=False)
    row["error"] = o.error or (o.fetch.error if o.fetch is not None and o.fetch.status not in (200, 304) else None)
    row["age_hours"] = _age(run_ts, row["build_ts"])
    row["content_changed"] = changed
    if latest is not None:
        matched = logic.match_versions(previous, latest) if previous else None
        s = logic.feed_summary(previous, latest, matched)
        row.update(version_id=latest.version_id, jobs=s.vacancies_total, employers=s.employers_total,
                   **_summary_fields(s))
    row["not_reaching"] = gap_count if export_usable and latest is not None else None
    return row
