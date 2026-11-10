"""BigQuery integration test for the write ordering, against feed_health_dev. Opt-in, because
it needs the collector key and network: FEED_HEALTH_BQ_TESTS=1 venv/bin/pytest tests/test_store_bq.py

It writes rows under a unique test source dated 2020 (so they are never "latest") and deletes
them afterwards.
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

import pytest

from feed_health import pipeline, store
from feed_health.config import load_settings
from feed_health.feeds import FetchResult, parse_feed
from feed_health.pipeline import Observation
from tests.conftest import feed_xml, jgp_job

pytestmark = pytest.mark.skipif(os.environ.get("FEED_HEALTH_BQ_TESTS") != "1", reason="set FEED_HEALTH_BQ_TESTS=1")

RUN = datetime(2020, 1, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def env():
    settings = load_settings(require_urls=False)
    assert settings.bq_dataset == "feed_health_dev", "integration tests only run against the dev dataset"
    client = store.client_for(settings)
    source = f"zz_test_{uuid.uuid4().hex[:8]}.xml"
    yield settings, client, source
    t = settings.table
    client.query(f"""
        DELETE FROM `{t('version_jobs')}` WHERE source = '{source}';
        DELETE FROM `{t('employer_checks')}` WHERE source = '{source}';
        DELETE FROM `{t('feed_versions')}` WHERE source = '{source}';
        DELETE FROM `{t('checks')}` WHERE check_ts = TIMESTAMP('{RUN.isoformat()}');
    """).result()


def count(client, settings, table, source):
    return next(iter(client.query(
        f"SELECT COUNT(*) AS n FROM `{settings.table(table)}` WHERE source = '{source}'").result()))["n"]


def test_crash_before_marker_then_rerun_leaves_one_copy(env):
    settings, client, source = env
    parsed = parse_feed(feed_xml(jgp_job("a1"), jgp_job("a2"), build="2020-01-01 06:00:00"), RUN)
    fetch = FetchResult(200, b"", '"e1"', "1577858400000000", datetime(2020, 1, 1, 6, tzinfo=timezone.utc), None)
    out = pipeline.compute_run(RUN, settings, {}, [Observation(source, settings.feed(source), fetch, parsed)], None)
    vid = out.new_versions[source].version_id

    with pytest.raises(RuntimeError, match="simulated crash"):
        store.write_run(client, settings, out, {}, crash_after_version_rows=True)
    assert count(client, settings, "version_jobs", source) == 2      # rows written...
    assert count(client, settings, "feed_versions", source) == 0     # ...but no marker
    states, _, _, _ = store.read_state(client, settings)
    assert source not in states or states[source].latest is None     # so readers don't see it

    store.write_run(client, settings, out, {})                       # the rerun
    assert count(client, settings, "version_jobs", source) == 2      # orphans replaced, not doubled
    assert count(client, settings, "employer_checks", source) == 1
    assert count(client, settings, "feed_versions", source) == 1
    states, _, _, _ = store.read_state(client, settings)
    latest = states[source].latest
    assert latest.version_id == vid and sorted(j.job_id for j in latest.jobs) == ["a1", "a2"]
    assert states[source].last_check.etag == '"e1"'


def test_new_vacancies_count_each_vacancy_once():
    """v_new_vacancies' query over temp tables (nothing is written): moves, re-created listings,
    cross-feed copies, non-imported feeds, UK days and the start of history."""
    settings = load_settings(require_urls=False)
    client = store.client_for(settings)

    def ts(value: str) -> str:
        return f"TIMESTAMP '{value}+00'"

    def listing(entity, ext, published, first_seen=None):
        ext_sql = f"'{ext}'" if ext else "CAST(NULL AS STRING)"
        return (f"STRUCT('{entity}' AS entity_id, {ext_sql} AS external_id, {ts(published)} AS published_ts, "
                f"{ts(first_seen or published)} AS first_seen_ts)")

    def feed_job(job, source, first_seen):
        return f"STRUCT('{job}' AS job_id, '{source}' AS source, {ts(first_seen)} AS first_seen_ts)"

    checks = ", ".join(f"STRUCT({ts('2026-11-10 12:00:00')} AS check_ts, '{s}' AS scope, '{label}' AS label, "
                       f"{imported} AS imported)" for s, label, imported in [
                           ("ats.xml", "ATS", "TRUE"), ("v3.xml", "V3", "TRUE"), ("scrape.xml", "Scrape", "TRUE"),
                           ("tribepad.xml", "Tribepad", "FALSE"), ("ALL", "All feeds", "CAST(NULL AS BOOL)")])
    jobs = ", ".join([
        feed_job("a1", "ats.xml", "2026-11-10 07:00:00"), feed_job("a2", "ats.xml", "2026-09-29 12:00:00"),
        feed_job("a3", "ats.xml", "2026-10-06 22:00:00"), feed_job("a4", "ats.xml", "2026-10-08 07:00:00"),
        feed_job("m1", "v3.xml", "2026-10-01 06:00:00"), feed_job("m1", "scrape.xml", "2026-10-05 06:00:00"),
        feed_job("s1", "tribepad.xml", "2026-10-01 06:00:00"), feed_job("s1", "scrape.xml", "2026-10-02 06:00:00"),
        feed_job("x1", "ats.xml", "2026-10-03 06:00:00"), feed_job("x2", "v3.xml", "2026-10-03 06:00:00")])
    listings = ", ".join([
        listing("1", "a1", "2026-11-10 07:30:00"),
        listing("2", "m1", "2026-10-01 09:00:00"),
        listing("3", "m1", "2026-10-05 09:00:00"),                      # re-created after a move: not new
        listing("4", "s1", "2026-10-02 09:00:00"),                      # Tribepad isn't imported: Scrape's
        listing("5", "x2", "2026-10-03 08:00:00"),                      # two copies of one vacancy:
        listing("6", "x1", "2026-10-03 10:00:00"),                      # counted once, for the first (V3)
        listing("7", None, "2026-10-04 09:00:00"),                      # made on Jobiqo
        listing("8", "zz", "2026-10-04 10:00:00"),                      # id in no monitored feed
        listing("9", "a2", "2026-09-29 13:00:00"),                      # the monitor's first day: left out
        listing("10", "a3", "2026-10-06 23:30:00"),                     # 00:30 on 7 Oct in UK time
        listing("11", "a4", "2026-10-20 09:00:00", "2026-10-08 09:00:00")])   # <date> moved later
    script = f"""
CREATE TEMP TABLE checks AS SELECT * FROM UNNEST([{checks}]);
CREATE TEMP TABLE feed_versions AS SELECT {ts('2026-09-29 11:48:00')} AS build_ts;
CREATE TEMP TABLE version_jobs AS SELECT * FROM UNNEST([{jobs}]);
CREATE TEMP TABLE duplicates AS SELECT * FROM UNNEST([
  STRUCT({ts('2026-10-03 12:00:00')} AS check_ts, 'g1' AS group_id, 'x1' AS job_id),
  STRUCT({ts('2026-10-03 12:00:00')} AS check_ts, 'g1' AS group_id, 'x2' AS job_id)]);
CREATE TEMP TABLE export_listings AS SELECT * FROM UNNEST([{listings}]);
SELECT * FROM ({store.new_vacancies_select(lambda name: name)}) ORDER BY day, feed;
"""
    rows = [(r["day"].isoformat(), r["feed"], r["new_vacancies"]) for r in client.query(script).result()]
    assert rows == [
        ("2026-10-01", "V3", 1), ("2026-10-02", "Scrape", 1), ("2026-10-03", "V3", 1),
        ("2026-10-04", "Made on Jobiqo", 1), ("2026-10-04", "Not in a monitored feed", 1),
        ("2026-10-07", "ATS", 1), ("2026-10-08", "ATS", 1), ("2026-11-10", "ATS", 1)]
