"""BigQuery: schema, reads and writes for the feed_health dataset.

Writes are load jobs only (never streaming inserts), because rows from a recent streaming
insert can't be deleted, and the crash cleanup below deletes rows. Commit order:
  per new version: version_jobs + employer_checks, then feed_versions (the version marker)
  per run:         jobiqo_gaps, then checks (the run marker, one load job)
Readers go through the markers: a version exists only once it is in feed_versions, and a
run's gaps count only if the run's checks rows exist. Before a version is written, any rows
left under its (deterministic) id by an interrupted run are deleted, so a rerun leaves one copy.
"""
from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

from feed_health.config import Settings
from feed_health.feeds import Job
from feed_health.logic import Version
from feed_health.pipeline import CHECK_FIELDS, LastCheck, RunOutput, SourceState

SCOPES = ["https://www.googleapis.com/auth/bigquery"]

F = bigquery.SchemaField
_TYPES = {"ts": "TIMESTAMP", "str": "STRING", "int": "INT64", "float": "FLOAT64", "bool": "BOOL"}


def _schema(*fields: tuple[str, str] | tuple[str, str, str]) -> list[bigquery.SchemaField]:
    return [F(f[0], _TYPES[f[1]], mode=f[2] if len(f) > 2 else "NULLABLE") for f in fields]


_CHECK_TYPES = {
    "check_ts": "ts", "last_modified": "ts", "build_ts": "ts", "last_build_date": "ts",
    "configured": "bool", "collect": "bool", "show": "bool", "imported": "bool", "rebuilt": "bool", "content_changed": "bool",
    "http_status": "int", "jobs": "int", "employers": "int", "duplicate_ids": "int", "employers_added": "int",
    "employers_removed": "int", "vacancies_added": "int", "vacancies_removed": "int", "not_reaching": "int",
    "errors": "int", "cluster_conflicts": "int", "live": "int", "duplicates": "int",
    "age_hours": "float", "orange_hours": "float", "red_hours": "float", "run_seconds": "float",
}

SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    "checks": _schema(*((f, _CHECK_TYPES.get(f, "str"), "REQUIRED" if f in ("check_ts", "scope") else "NULLABLE")
                        for f in CHECK_FIELDS)),
    "feed_versions": _schema(
        ("version_id", "str", "REQUIRED"), ("source", "str", "REQUIRED"), ("build_ts", "ts", "REQUIRED"),
        ("last_build_date", "ts"), ("first_seen_ts", "ts"), ("generation", "str"), ("content_hash", "str"),
        ("jobs", "int"), ("employers", "int"), ("prev_version_id", "str")),
    "version_jobs": _schema(
        ("version_id", "str", "REQUIRED"), ("source", "str", "REQUIRED"), ("build_ts", "ts", "REQUIRED"),
        ("job_id", "str", "REQUIRED"), ("reference", "str"), ("org_key", "str"), ("org_name", "str"),
        ("title", "str"), ("url_norm", "str"), ("close_date", "ts"), ("first_seen_ts", "ts")),
    "employer_checks": _schema(
        ("version_id", "str", "REQUIRED"), ("prev_version_id", "str"), ("source", "str", "REQUIRED"),
        ("build_ts", "ts", "REQUIRED"), ("prev_build_ts", "ts"), ("org_key", "str", "REQUIRED"), ("org_name", "str"),
        ("still_showing", "bool"), ("jobs_now", "int"), ("jobs_prev", "int"), ("added", "int"), ("expired", "int"),
        ("moved", "int"), ("removed_unexpected", "int"), ("removed_no_close", "int"), ("rehashed", "int")),
    "jobiqo_gaps": _schema(
        ("check_ts", "ts", "REQUIRED"), ("source", "str", "REQUIRED"), ("org_key", "str"), ("org_name", "str"),
        ("job_id", "str", "REQUIRED"), ("title", "str"), ("first_seen_ts", "ts"), ("close_date", "ts")),
    "employer_live": _schema(
        ("check_ts", "ts", "REQUIRED"), ("source", "str", "REQUIRED"), ("org_key", "str", "REQUIRED"), ("live", "int")),
    "duplicates": _schema(
        ("check_ts", "ts", "REQUIRED"), ("group_id", "str", "REQUIRED"), ("basis", "str"), ("source", "str", "REQUIRED"),
        ("job_id", "str", "REQUIRED"), ("org_key", "str"), ("org_name", "str"), ("title", "str"), ("live", "bool")),
}
PARTITION_FIELD = {"version_jobs": "build_ts", "jobiqo_gaps": "check_ts", "employer_live": "check_ts",
                   "duplicates": "check_ts"}
CLUSTER_FIELDS = {"version_jobs": ["source", "org_key"], "jobiqo_gaps": ["source"], "employer_live": ["source"],
                  "duplicates": ["source"]}


def client_for(settings: Settings) -> bigquery.Client:
    """Reads use BigQuery's short-query mode (no job is created for small queries), which cuts
    the round trips that dominate an hourly run where nothing changed."""
    creds = Credentials.from_service_account_file(str(settings.credentials_path), scopes=SCOPES) \
        if settings.credentials_path else None
    return bigquery.Client(project=settings.bq_project, credentials=creds, location=settings.bq_location,
                           default_job_creation_mode=bigquery.enums.JobCreationMode.JOB_CREATION_OPTIONAL)


# --------------------------------------------------------------------------- schema

def ensure_schema(client: bigquery.Client, settings: Settings) -> list[str]:
    """Create missing tables, add missing (nullable) columns to existing ones, and (re)create the
    views. Idempotent, so it doubles as the migration; run by `feed_health.setup`, not by every
    collector run (it costs several seconds of API calls). Run it BEFORE deploying code that
    writes new columns, or that code's load jobs fail."""
    done = []
    for name, schema in SCHEMAS.items():
        try:
            existing = client.get_table(settings.table(name))
        except NotFound:
            table = bigquery.Table(settings.table(name), schema=schema)
            if name in PARTITION_FIELD:
                table.time_partitioning = bigquery.TimePartitioning(field=PARTITION_FIELD[name])
            if name in CLUSTER_FIELDS:
                table.clustering_fields = CLUSTER_FIELDS[name]
            client.create_table(table)
            done.append(f"{name} (created)")
            continue
        have = {f.name for f in existing.schema}
        missing = [f for f in schema if f.name not in have]
        if missing:
            existing.schema = [*existing.schema, *missing]
            client.update_table(existing, ["schema"])
            done.append(f"{name} (+{', '.join(f.name for f in missing)})")
        else:
            done.append(name)
    for name, sql in view_sql(settings).items():
        client.query(sql).result()
        done.append(name)
    return done


def view_sql(settings: Settings) -> dict[str, str]:
    t = settings.table
    return {
        "v_feed_status": f"""
CREATE OR REPLACE VIEW `{t('v_feed_status')}` AS
-- The latest run's row per scope, with its staleness colour. Colours use the thresholds that
-- were configured at that check; the feed map row is informational (no thresholds).
SELECT
  scope, label, configured, collect, show, imported, check_ts, build_ts, last_build_date, age_hours,
  orange_hours, red_hours,
  CASE
    WHEN error IS NOT NULL THEN 'error'
    WHEN orange_hours IS NULL THEN NULL
    WHEN age_hours IS NULL THEN 'unknown'
    WHEN age_hours > red_hours THEN 'red'
    WHEN age_hours > orange_hours THEN 'orange'
    ELSE 'green'
  END AS status,
  http_status, error, note, jobs, employers, live, duplicates, version_id, rebuilt, content_changed,
  export_status, errors, cluster_conflicts, run_seconds
FROM `{t('checks')}`
WHERE check_ts = (SELECT MAX(check_ts) FROM `{t('checks')}`)""",

        "v_summary": f"""
CREATE OR REPLACE VIEW `{t('v_summary')}` AS
-- The latest run's summary: the all-feeds total plus each configured, shown feed. Added and
-- removed compare each feed's latest version (this_version_ts) with the one before it
-- (prev_version_ts); NULL deltas mean the feed has only one version so far.
WITH versions AS (
  SELECT source, build_ts,
         ROW_NUMBER() OVER (PARTITION BY source ORDER BY build_ts DESC, first_seen_ts DESC) AS rn
  FROM `{t('feed_versions')}`
)
SELECT
  c.check_ts, c.scope, c.label,
  c.employers AS employers_total, c.employers_added, c.employers_removed,
  c.jobs AS vacancies_total, c.vacancies_added, c.vacancies_removed,
  c.live, c.not_reaching, c.duplicates, c.export_status,
  cur.build_ts AS this_version_ts, prev.build_ts AS prev_version_ts
FROM `{t('checks')}` c
LEFT JOIN versions cur ON cur.source = c.scope AND cur.rn = 1
LEFT JOIN versions prev ON prev.source = c.scope AND prev.rn = 2
WHERE c.check_ts = (SELECT MAX(check_ts) FROM `{t('checks')}`)
  AND (c.scope = 'ALL' OR (c.configured AND c.collect AND c.show))""",

        "v_employers": f"""
CREATE OR REPLACE VIEW `{t('v_employers')}` AS
-- One row per employer per feed for each feed's latest version (joined through the
-- feed_versions marker), plus employers seen in the last 14 days that are absent now.
WITH run AS (
  SELECT MAX(check_ts) AS ts FROM `{t('checks')}`
),
cfg AS (
  SELECT c.scope AS source, c.label, c.show, c.imported, c.configured
  FROM `{t('checks')}` c, run WHERE c.check_ts = run.ts
),
latest AS (
  SELECT source, version_id, build_ts FROM `{t('feed_versions')}`
  WHERE TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY source ORDER BY build_ts DESC, first_seen_ts DESC) = 1
),
current_rows AS (
  SELECT e.* FROM `{t('employer_checks')}` e JOIN latest l USING (source, version_id)
),
gaps AS (
  SELECT g.source, g.org_key, COUNT(*) AS not_reaching
  FROM `{t('jobiqo_gaps')}` g, run
  WHERE g.check_ts = run.ts
  GROUP BY 1, 2
),
live AS (
  SELECT el.source, el.org_key, el.live AS on_jobiqo
  FROM `{t('employer_live')}` el, run
  WHERE el.check_ts = run.ts
),
seen AS (
  SELECT vj.source, vj.org_key, ANY_VALUE(vj.org_name) AS org_name, MAX(vj.build_ts) AS last_seen_ts
  FROM `{t('version_jobs')}` vj
  JOIN `{t('feed_versions')}` fv USING (version_id)
  WHERE vj.build_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
  GROUP BY 1, 2
)
SELECT
  cfg.label, r.source, r.org_key, r.org_name, r.still_showing,
  IF(r.still_showing, NULL, s.last_seen_ts) AS last_seen_ts,
  r.jobs_prev, r.jobs_now, live.on_jobiqo, r.added, r.expired, r.removed_unexpected, r.removed_no_close,
  r.moved, r.rehashed,
  IF(cfg.imported, COALESCE(g.not_reaching, 0), NULL) AS not_reaching,
  r.build_ts AS this_check_ts, r.prev_build_ts AS last_check_ts,
  cfg.show, cfg.imported, cfg.configured
FROM current_rows r
LEFT JOIN seen s USING (source, org_key)
LEFT JOIN gaps g USING (source, org_key)
LEFT JOIN live USING (source, org_key)
LEFT JOIN cfg USING (source)
UNION ALL
SELECT
  cfg.label, s.source, s.org_key, s.org_name, FALSE, s.last_seen_ts,
  NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
  NULL, l.build_ts, NULL,
  cfg.show, cfg.imported, cfg.configured
FROM seen s
JOIN latest l USING (source)
LEFT JOIN current_rows r USING (source, org_key)
LEFT JOIN cfg USING (source)
WHERE r.org_key IS NULL""",

        "v_duplicates": f"""
CREATE OR REPLACE VIEW `{t('v_duplicates')}` AS
-- The latest run's vacancies listed in two or more shown, imported feeds: one row per copy.
-- Each copy that Jobiqo imports is a separate advert on the board (live = TRUE).
WITH run AS (
  SELECT MAX(check_ts) AS ts FROM `{t('checks')}`
),
cfg AS (
  SELECT c.scope AS source, c.label FROM `{t('checks')}` c, run WHERE c.check_ts = run.ts
)
SELECT d.group_id, d.basis, d.source, cfg.label, d.job_id, d.org_key, d.org_name, d.title, d.live, d.check_ts
FROM `{t('duplicates')}` d
JOIN run ON d.check_ts = run.ts
LEFT JOIN cfg USING (source)""",
    }


# --------------------------------------------------------------------------- reads

def read_state(client: bigquery.Client, settings: Settings) -> tuple[dict[str, SourceState], int | None]:
    """Everything the next run needs: each source's latest two committed versions (with jobs)
    and its last check row; plus the previous Jobiqo export job count."""
    t = settings.table
    with ThreadPoolExecutor(max_workers=2) as pool:  # independent reads: run them side by side
        versions_future = pool.submit(lambda: list(client.query_and_wait(f"""
            SELECT * FROM `{t('feed_versions')}` WHERE TRUE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY source ORDER BY build_ts DESC, first_seen_ts DESC) <= 2
        """)))
        checks_future = pool.submit(lambda: list(client.query_and_wait(f"""
            SELECT * FROM `{t('checks')}` WHERE TRUE
            QUALIFY ROW_NUMBER() OVER (PARTITION BY scope ORDER BY check_ts DESC) = 1
        """)))
        versions = versions_future.result()
        last_checks = {r["scope"]: r for r in checks_future.result()}

    jobs_by_version: dict[str, list] = defaultdict(list)
    if versions:
        params = [bigquery.ArrayQueryParameter("ids", "STRING", [v["version_id"] for v in versions]),
                  bigquery.ArrayQueryParameter("days", "DATE", sorted({v["build_ts"].date() for v in versions}))]
        for r in client.query_and_wait(f"""
            SELECT version_id, job_id, reference, title, org_key, org_name, url_norm, close_date, first_seen_ts
            FROM `{t('version_jobs')}`
            WHERE version_id IN UNNEST(@ids) AND DATE(build_ts) IN UNNEST(@days)
        """, job_config=bigquery.QueryJobConfig(query_parameters=params)):
            jobs_by_version[r["version_id"]].append(r)

    ranked: dict[str, list] = defaultdict(list)
    for v in sorted(versions, key=lambda r: (r["build_ts"], r["first_seen_ts"]), reverse=True):
        ranked[v["source"]].append(v)

    states: dict[str, SourceState] = {}
    for source in ranked.keys() | (last_checks.keys() - {"ALL", "feed_map", "jobiqo_export"}):
        rows = ranked.get(source, [])
        latest = _version(rows[0], jobs_by_version) if rows else None
        previous = _version(rows[1], jobs_by_version) if len(rows) > 1 else None
        states[source] = SourceState(latest=latest, latest_hash=rows[0]["content_hash"] if rows else None,
                                     previous=previous, last_check=_last_check(last_checks.get(source)))
    export = last_checks.get("jobiqo_export")
    return states, (export["jobs"] if export else None)


def _version(meta, jobs_by_version) -> Version:
    rows = jobs_by_version.get(meta["version_id"], [])
    jobs = tuple(Job(job_id=r["job_id"], reference=r["reference"] or "", title=r["title"] or "",
                     org_key=r["org_key"] or "none:", org_name=r["org_name"] or "",
                     url_norm=r["url_norm"] or "", close_date=r["close_date"]) for r in rows)
    first = {r["job_id"]: r["first_seen_ts"] for r in rows if r["first_seen_ts"]}
    return Version(meta["source"], meta["version_id"], meta["build_ts"], jobs, first)


def _last_check(row) -> LastCheck | None:
    if row is None:
        return None
    return LastCheck(row["etag"], row["generation"], row["last_modified"], row["build_ts"],
                     row["last_build_date"], row["duplicate_ids"], row["jobs"])


# --------------------------------------------------------------------------- writes

def _jsonable(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _load(client: bigquery.Client, settings: Settings, table: str, rows: list[dict]) -> bigquery.LoadJob:
    config = bigquery.LoadJobConfig(schema=SCHEMAS[table], write_disposition="WRITE_APPEND",
                                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    return client.load_table_from_json([{k: _jsonable(v) for k, v in r.items()} for r in rows],
                                       settings.table(table), job_config=config)


def _wait(*jobs) -> None:
    for job in jobs:
        job.result()


def delete_orphans(client: bigquery.Client, settings: Settings, versions: list[Version]) -> None:
    t = settings.table
    params = [bigquery.ArrayQueryParameter("ids", "STRING", [v.version_id for v in versions]),
              bigquery.ArrayQueryParameter("days", "DATE", sorted({v.build_ts.date() for v in versions}))]
    client.query(f"""
        DELETE FROM `{t('version_jobs')}` WHERE version_id IN UNNEST(@ids) AND DATE(build_ts) IN UNNEST(@days);
        DELETE FROM `{t('employer_checks')}` WHERE version_id IN UNNEST(@ids);
    """, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()


def version_rows(out: RunOutput, states: dict[str, SourceState]) -> tuple[list[dict], list[dict]]:
    """(version_jobs rows, feed_versions rows) for this run's new versions."""
    job_rows, version_meta = [], []
    for source, v in out.new_versions.items():
        prev = states.get(source, SourceState()).latest
        for j in v.jobs:
            job_rows.append({"version_id": v.version_id, "source": source, "build_ts": v.build_ts,
                             "job_id": j.job_id, "reference": j.reference, "org_key": j.org_key,
                             "org_name": j.org_name, "title": j.title, "url_norm": j.url_norm,
                             "close_date": j.close_date, "first_seen_ts": v.first_seen_of(j.job_id)})
        m = out.new_version_meta[source]
        version_meta.append({"version_id": v.version_id, "source": source, "build_ts": v.build_ts,
                             "last_build_date": m["last_build_date"], "first_seen_ts": m["first_seen_ts"],
                             "generation": m["generation"], "content_hash": m["content_hash"],
                             "jobs": len(v.jobs), "employers": len({j.org_key for j in v.jobs}),
                             "prev_version_id": prev.version_id if prev else None})
    return job_rows, version_meta


def write_run(client: bigquery.Client, settings: Settings, out: RunOutput,
              states: dict[str, SourceState], crash_after_version_rows: bool = False) -> None:
    """Write one run in commit order. `crash_after_version_rows` exists only so the integration
    test can simulate a run dying before its markers are written."""
    if out.new_versions:
        delete_orphans(client, settings, list(out.new_versions.values()))
        job_rows, version_meta = version_rows(out, states)
        employer = [e.__dict__ for e in out.employer_rows]
        _wait(*[_load(client, settings, name, rows) for name, rows in
                (("version_jobs", job_rows), ("employer_checks", employer)) if rows])
        if crash_after_version_rows:
            raise RuntimeError("simulated crash before the feed_versions marker")
        _wait(_load(client, settings, "feed_versions", version_meta))
    run_ts = out.checks[0]["check_ts"]
    per_run = {
        "jobiqo_gaps": [{"check_ts": run_ts, "source": g.source, "org_key": g.org_key, "org_name": g.org_name,
                         "job_id": g.job_id, "title": g.title, "first_seen_ts": g.first_seen,
                         "close_date": g.close_date} for g in out.gaps],
        "employer_live": out.employer_live,
        "duplicates": out.duplicates,
    }
    _wait(*[_load(client, settings, name, rows) for name, rows in per_run.items() if rows])
    _wait(_load(client, settings, "checks", out.checks))
