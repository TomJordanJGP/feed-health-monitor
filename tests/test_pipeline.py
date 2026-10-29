from datetime import datetime, timedelta, timezone

import pytest

from feed_health import pipeline
from feed_health.config import load_settings
from feed_health.feeds import ExportOrg, FetchResult, JobiqoExport, parse_feed
from feed_health.pipeline import (ALL_SCOPE, EXPORT_SCOPE, HUBSPOT_SCOPE, ExportObservation, LastCheck, Observation,
                                  SourceState)
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


TRIBEPAD, NEW_FEED = "jgp_tribepad_feed.xml", "brand_new_feed.xml"


def tribepad_and_new_feed_run(settings, export_obs=None, prev_export_jobs=None):
    """ATS (a1, a2), Tribepad (t1-t3, not imported) and a feed missing from feeds.yml (n1)."""
    states = {ATS: first_run(settings),
              TRIBEPAD: first_run(settings, TRIBEPAD, feed_xml(jgp_job("t1"), jgp_job("t2"), jgp_job("t3"))),
              NEW_FEED: first_run(settings, NEW_FEED, feed_xml(jgp_job("n1")))}
    unchanged = FetchResult(304, None, None, None, None, None)
    obs = [Observation(s, settings.feed(s), unchanged) for s in states]
    return pipeline.compute_run(RUN, settings, states, obs, export_obs, prev_export_jobs=prev_export_jobs)


def test_totals_cover_every_shown_imported_feed_including_unlisted_ones(settings):
    out = tribepad_and_new_feed_run(settings)
    assert row(out, ALL_SCOPE)["jobs"] == 3       # ATS + the unlisted feed; Tribepad isn't imported
    new = row(out, NEW_FEED)
    assert (new["configured"], new["imported"], new["show"]) == (False, True, True)
    tribepad = row(out, TRIBEPAD)
    assert (tribepad["show"], tribepad["imported"]) == (True, False)


def test_tribepad_jobs_missing_from_jobiqo_stay_out_of_the_totals(settings):
    export = JobiqoExport(ids=frozenset({"a1", "a2", "n1"}), company_by_id={}, jobs=3)
    export_obs = ExportObservation(FetchResult(200, b"", None, None, RUN, None), export, None)
    out = tribepad_and_new_feed_run(settings, export_obs, prev_export_jobs=3)
    assert row(out, TRIBEPAD)["not_reaching"] == 3     # recorded per feed; v_employers calls it not_imported
    assert sorted(g.job_id for g in out.gaps) == ["t1", "t2", "t3"]
    all_row = row(out, ALL_SCOPE)
    assert (all_row["not_reaching"], all_row["live"]) == (0, 3)


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


def test_export_orgs_link_to_the_feed_employer_most_of_their_jobs_come_from(settings):
    xml = feed_xml(jgp_job("a1", org_id="934", org_name="Sefton Council"),
                   jgp_job("a2", org_id="934", org_name="Sefton Council"),
                   jgp_job("b1", org_id="77"), jgp_job("b2", org_id="88"))
    ats = first_run(settings, xml=xml)
    export = JobiqoExport(
        ids=frozenset({"a1", "a2", "b1", "b2"}), company_by_id={}, jobs=7, organizations=3,
        orgs={"1047": ExportOrg("1047", "Sefton Council", 3, 1), "3000": ExportOrg("3000", "Split Trust", 2, 0),
              "4000": ExportOrg("4000", "Direct Only Ltd", 2, 2)},
        org_by_id={"a1": "1047", "a2": "1047", "b1": "3000", "b2": "3000"})
    export_obs = ExportObservation(FetchResult(200, b"", None, None, RUN, None), export, None)
    unchanged = FetchResult(304, None, None, None, None, None)
    out = pipeline.compute_run(RUN, settings, {ATS: ats}, [Observation(ATS, settings.feed(ATS), unchanged)],
                               export_obs, prev_export_jobs=7)
    links = {r["export_org_id"]: (r["org_key"], r["link_share"], r["jobs"], r["site_jobs"]) for r in out.export_orgs}
    assert links == {"1047": ("jgp:934", 1.0, 3, 1),
                     "3000": (None, None, 2, 0),      # 50/50 between two employers: below the 60% rule
                     "4000": (None, None, 2, 2)}      # only jobs created on Jobiqo: nothing to link through


def test_no_export_orgs_from_an_untrusted_export(settings):
    st = first_run(settings)
    export = JobiqoExport(ids=frozenset(), company_by_id={}, jobs=3000, orgs={"1": ExportOrg("1", "X", 3000, 0)})
    export_obs = ExportObservation(FetchResult(200, b"", None, None, RUN, None), export, None)
    out = pipeline.compute_run(RUN, settings, {ATS: st}, [Observation(ATS, settings.feed(ATS), FetchResult(304, None, None, None, None, None))],
                               export_obs, prev_export_jobs=7800)
    assert out.export_orgs == []


def test_hubspot_sync_is_due_once_a_day_after_six_utc():
    def at(hour, days_ago=0):
        return datetime(2026, 10, 30, hour, tzinfo=UTC) - timedelta(days=days_ago)
    assert pipeline.hubspot_due(at(7), None)                       # never synced
    assert pipeline.hubspot_due(at(7), at(5))                      # last sync before today's 06:00
    assert not pipeline.hubspot_due(at(9), at(7))                  # already synced today
    assert not pipeline.hubspot_due(at(5), at(7, days_ago=1))      # before 06:00, yesterday's sync still counts
    assert pipeline.hubspot_due(at(5), at(5, days_ago=1))          # ...unless yesterday's was missed


def test_hubspot_row_keeps_the_last_good_sync_while_later_ones_fail():
    synced = pipeline.hubspot_row(RUN, None, synced=8972, note="synced this run")
    assert (synced["scope"], synced["build_ts"], synced["employers"], synced["error"], synced["age_hours"]) == \
        (HUBSPOT_SCOPE, RUN, 8972, None, 0)
    assert (synced["orange_hours"], synced["red_hours"]) == (30.0, 54.0)
    failed = pipeline.hubspot_row(RUN + timedelta(hours=3), synced, error="HubSpot HTTP 401 (INVALID_AUTHENTICATION)")
    assert (failed["build_ts"], failed["employers"], failed["age_hours"]) == (RUN, 8972, 3.0)
    quiet = pipeline.hubspot_row(RUN + timedelta(hours=4), failed)     # a run that didn't try
    assert (quiet["error"], quiet["note"], quiet["age_hours"]) == (failed["error"], None, 4.0)
    assert pipeline.hubspot_row(RUN, None)["build_ts"] is None          # never synced: no age, no colour
