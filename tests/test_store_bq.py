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
    states, _ = store.read_state(client, settings)
    assert source not in states or states[source].latest is None     # so readers don't see it

    store.write_run(client, settings, out, {})                       # the rerun
    assert count(client, settings, "version_jobs", source) == 2      # orphans replaced, not doubled
    assert count(client, settings, "employer_checks", source) == 1
    assert count(client, settings, "feed_versions", source) == 1
    states, _ = store.read_state(client, settings)
    latest = states[source].latest
    assert latest.version_id == vid and sorted(j.job_id for j in latest.jobs) == ["a1", "a2"]
    assert states[source].last_check.etag == '"e1"'
