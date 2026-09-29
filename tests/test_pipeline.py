from datetime import datetime, timedelta, timezone

import pytest

from feed_health import pipeline
from feed_health.config import load_settings
from feed_health.feeds import FetchResult, JobiqoExport, parse_feed
from feed_health.pipeline import ALL_SCOPE, EXPORT_SCOPE, ExportObservation, LastCheck, Observation, SourceState
from tests.conftest import feed_xml, jgp_job

UTC = timezone.utc
RUN = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)
BUILT_EARLIER = datetime(2026, 9, 29, 6, 55, tzinfo=UTC)
ATS = "jgp_ats_feed.xml"


@pytest.fixture
def settings():
    return load_settings(require_urls=False)


def ok(generation="g2", built=datetime(2026, 9, 29, 11, 0, tzinfo=UTC)):
    return FetchResult(200, b"", '"etag-2"', generation, built, None)


def first_run(settings, source=ATS, xml=None):
    """A source's first version, as the state the next run starts from."""
    parsed = parse_feed(xml or feed_xml(jgp_job("a1"), jgp_job("a2")), RUN)
    obs = Observation(source, settings.feed(source), FetchResult(200, b"", '"etag-1"', "g1", BUILT_EARLIER, None), parsed)
    out = pipeline.compute_run(RUN - timedelta(hours=5), settings, {}, [obs], None)
    v = out.latest[source]
    return SourceState(latest=v, latest_hash=parsed.content_hash, previous=None,
                       last_check=LastCheck('"etag-1"', "g1", BUILT_EARLIER, BUILT_EARLIER, parsed.build_date, 0, 2))


def row(out, scope):
    (r,) = [c for c in out.checks if c["scope"] == scope]
    return r


def test_unchanged_304_carries_build_forward_and_age_grows(settings):
    st = first_run(settings)
    out = pipeline.compute_run(RUN, settings, {ATS: st}, [Observation(ATS, settings.feed(ATS), FetchResult(304, None, None, None, None, None))], None)
    r = row(out, ATS)
    assert (r["build_ts"], r["content_changed"], r["rebuilt"], r["version_id"]) == (BUILT_EARLIER, False, False, st.latest.version_id)
    assert r["age_hours"] == pytest.approx(5.083, abs=0.01) and r["error"] is None
    assert out.new_versions == {}


def test_fetch_error_keeps_last_build_so_staleness_still_shows(settings):
    st = first_run(settings)
    failed = FetchResult(0, None, None, None, None, "request failed: ConnectTimeout")
    r = row(pipeline.compute_run(RUN, settings, {ATS: st}, [Observation(ATS, settings.feed(ATS), failed)], None), ATS)
    assert r["error"] == "request failed: ConnectTimeout" and r["build_ts"] == BUILT_EARLIER and r["jobs"] == 2


def test_identical_rebuild_moves_build_time_but_makes_no_version(settings):
    st = first_run(settings)
    parsed = parse_feed(feed_xml(jgp_job("a2"), jgp_job("a1"), build="2026-09-29 11:00:00"), RUN)
    out = pipeline.compute_run(RUN, settings, {ATS: st}, [Observation(ATS, settings.feed(ATS), ok(), parsed)], None)
    r = row(out, ATS)
    assert out.new_versions == {} and (r["rebuilt"], r["content_changed"]) == (True, False)
    assert r["build_ts"] == datetime(2026, 9, 29, 11, 0, tzinfo=UTC)


def test_changed_content_makes_a_version_with_deterministic_id(settings):
    st = first_run(settings)
    parsed = parse_feed(feed_xml(jgp_job("a1"), jgp_job("a3")), RUN)
    out = pipeline.compute_run(RUN, settings, {ATS: st}, [Observation(ATS, settings.feed(ATS), ok(), parsed)], None)
    assert list(out.new_versions) == [ATS] and out.new_versions[ATS].version_id == f"{ATS}@g2"
    r = row(out, ATS)
    assert (r["vacancies_added"], r["vacancies_removed"], r["content_changed"]) == (1, 1, True)


def test_untrusted_export_disables_not_reaching_everywhere(settings):
    st = first_run(settings)
    export = JobiqoExport(ids=frozenset(), company_by_id={}, jobs=3000)
    export_obs = ExportObservation(FetchResult(200, b"", None, None, RUN, None), export, None)
    out = pipeline.compute_run(RUN, settings, {ATS: st}, [Observation(ATS, settings.feed(ATS), FetchResult(304, None, None, None, None, None))],
                               export_obs, prev_export_jobs=7800)
    assert row(out, EXPORT_SCOPE)["export_status"].startswith("export shrank")
    assert row(out, ATS)["not_reaching"] is None and row(out, ALL_SCOPE)["not_reaching"] is None
    assert out.gaps == []


def test_totals_cover_only_shown_imported_configured_feeds(settings):
    ats = first_run(settings)
    tribepad = first_run(settings, "jgp_tribepad_feed.xml", feed_xml(jgp_job("t1"), jgp_job("t2"), jgp_job("t3")))
    stranger = first_run(settings, "brand_new_feed.xml", feed_xml(jgp_job("n1")))
    unchanged = FetchResult(304, None, None, None, None, None)
    obs = [Observation(s, settings.feed(s), unchanged, note="not listed in feed_map.xml" if s == "brand_new_feed.xml" else None)
           for s in (ATS, "jgp_tribepad_feed.xml", "brand_new_feed.xml")]
    out = pipeline.compute_run(RUN, settings, {ATS: ats, "jgp_tribepad_feed.xml": tribepad, "brand_new_feed.xml": stranger}, obs, None)
    assert row(out, ALL_SCOPE)["jobs"] == 2       # ATS only: Tribepad isn't imported, the new feed isn't configured
    assert row(out, "brand_new_feed.xml")["configured"] is False
    assert row(out, "jgp_tribepad_feed.xml")["show"] is False


def test_live_duplicates_and_export_employers(settings):
    ats = first_run(settings, xml=feed_xml(jgp_job("a1", url="https://j.example/1"), jgp_job("a2")))
    v1 = first_run(settings, "jgp_backfill_feed_v1.xml", feed_xml(jgp_job("b1", url="https://j.example/1")))
    export = JobiqoExport(ids=frozenset({"a1", "b1"}), company_by_id={}, jobs=3, organizations=2)
    export_obs = ExportObservation(FetchResult(200, b"", None, None, RUN, None), export, None)
    unchanged = FetchResult(304, None, None, None, None, None)
    obs = [Observation(s, settings.feed(s), unchanged) for s in (ATS, "jgp_backfill_feed_v1.xml")]
    out = pipeline.compute_run(RUN, settings, {ATS: ats, "jgp_backfill_feed_v1.xml": v1}, obs, export_obs,
                               prev_export_jobs=3)
    assert row(out, ATS)["live"] == 1 and row(out, "jgp_backfill_feed_v1.xml")["live"] == 1
    assert (row(out, ALL_SCOPE)["live"], row(out, ALL_SCOPE)["duplicates"]) == (2, 1)
    assert row(out, EXPORT_SCOPE)["employers"] == 2
    assert sum(r["live"] for r in out.employer_live) == 2
    assert sorted((d["source"], d["live"]) for d in out.duplicates) == [(ATS, True), ("jgp_backfill_feed_v1.xml", True)]


def test_no_trusted_export_means_live_unknown_not_zero(settings):
    st = first_run(settings)
    out = pipeline.compute_run(RUN, settings, {ATS: st}, [Observation(ATS, settings.feed(ATS), FetchResult(304, None, None, None, None, None))], None)
    assert row(out, ATS)["live"] is None and row(out, ALL_SCOPE)["live"] is None and out.employer_live == []
