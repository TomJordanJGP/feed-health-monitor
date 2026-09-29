from datetime import datetime, timedelta, timezone

import pytest

from feed_health import logic
from feed_health.feeds import Job, JobiqoExport
from feed_health.logic import (EXPIRED, MOVED, REMOVED_NO_CLOSE, REMOVED_UNEXPECTED, Clusters, DiffContext,
                               Version, all_feeds_summary, carry_first_seen, classify_gone, employer_checks,
                               export_problem, feed_summary, match_versions, not_reaching)

UTC = timezone.utc
BUILD_PREV = datetime(2026, 9, 28, 6, 55, tzinfo=UTC)
BUILD = datetime(2026, 9, 29, 6, 55, tzinfo=UTC)
RUN = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)
GRACE = timedelta(hours=24)


def job(job_id, org="jgp:116", *, ref="", url="", close=None, name="Surrey County Council", title="Role"):
    return Job(job_id=job_id, reference=ref, title=title, org_key=org, org_name=name, url_norm=url, close_date=close)


def version(source, vid, build, *jobs, first_seen=None):
    return Version(source=source, version_id=vid, build_ts=build, jobs=tuple(jobs), first_seen=first_seen or {})


def ctx_for(*versions, latest=None, imported=None):
    """Context whose clusters span all given versions. latest: source -> version_id."""
    latest = latest or {v.source: v.version_id for v in versions}
    imported = imported if imported is not None else {v.source for v in versions}
    return DiffContext(Clusters(versions), latest, frozenset(imported), GRACE)


def classify(prev, curr, *others, imported=None):
    ctx = ctx_for(prev, curr, *others,
                  latest={**{o.source: o.version_id for o in others}, curr.source: curr.version_id},
                  imported=imported)
    matched = match_versions(prev, curr)
    return {j.job_id: classify_gone(j, prev.source, prev.version_id, curr.build_ts, ctx)
            for j in prev.jobs if j.job_id not in matched}


class TestClassifyGone:
    def test_close_date_passed_is_expired(self):
        prev = version("f", "f@1", BUILD_PREV, job("a", close=datetime(2026, 9, 27, 23, tzinfo=UTC)))
        assert classify(prev, version("f", "f@2", BUILD)) == {"a": EXPIRED}

    def test_removed_on_its_closing_day_is_expired(self):
        prev = version("f", "f@1", BUILD_PREV, job("a", close=datetime(2026, 9, 29, 18, tzinfo=UTC)))
        assert classify(prev, version("f", "f@2", BUILD)) == {"a": EXPIRED}

    def test_removed_well_before_close_is_unexpected(self):
        prev = version("f", "f@1", BUILD_PREV, job("a", close=datetime(2026, 10, 10, tzinfo=UTC)))
        assert classify(prev, version("f", "f@2", BUILD)) == {"a": REMOVED_UNEXPECTED}

    def test_no_close_date_anywhere(self):
        prev = version("v1", "v1@1", BUILD_PREV, job("a"))
        assert classify(prev, version("v1", "v1@2", BUILD)) == {"a": REMOVED_NO_CLOSE}

    def test_no_close_date_borrows_twin_close_date(self):
        # Backfill V1 has no close dates; the same vacancy (org + reference) in Organic-1 does.
        prev = version("v1", "v1@1", BUILD_PREV, job("a", ref="LCC/123"))
        organic = version("v3", "v3@1", BUILD_PREV, job("z", ref="LCC/123", close=datetime(2026, 9, 26, tzinfo=UTC)))
        assert classify(prev, version("v1", "v1@2", BUILD), organic) == {"a": EXPIRED}

    def test_own_future_close_date_wins_over_twin_past_one(self):
        prev = version("f", "f@1", BUILD_PREV, job("a", ref="R-100", close=datetime(2026, 10, 10, tzinfo=UTC)))
        other = version("g", "g@1", BUILD_PREV, job("z", ref="R-100", close=datetime(2026, 9, 20, tzinfo=UTC)))
        # Twin exists but its version isn't g's latest, so not "moved" either.
        ctx = ctx_for(prev, other, latest={"g": "g@2", "f": "f@2"})
        got = classify_gone(prev.jobs[0], "f", "f@1", BUILD, ctx)
        assert got == REMOVED_UNEXPECTED

    def test_moved_when_twin_live_in_imported_feed(self):
        prev = version("f", "f@1", BUILD_PREV, job("a", url="https://x.example/j1", close=datetime(2026, 10, 10, tzinfo=UTC)))
        other = version("g", "g@3", BUILD, job("b", url="https://x.example/j1"))
        assert classify(prev, version("f", "f@2", BUILD), other) == {"a": MOVED}

    def test_twin_only_in_non_imported_feed_is_not_moved(self):
        # Tribepad shares ids with Scraping but Jobiqo doesn't import it.
        prev = version("scrape", "s@1", BUILD_PREV, job("a", close=datetime(2026, 10, 10, tzinfo=UTC)))
        tribepad = version("tribepad", "t@1", BUILD, job("a"))
        got = classify(prev, version("scrape", "s@2", BUILD), tribepad, imported={"scrape"})
        assert got == {"a": REMOVED_UNEXPECTED}


class TestMatchVersions:
    def test_rehash_by_unique_reference(self):
        prev = version("f", "f@1", BUILD_PREV, job("old-hash", ref="R-100"))
        curr = version("f", "f@2", BUILD, job("new-hash", ref="R-100"))
        assert match_versions(prev, curr) == {"old-hash": ("new-hash", True)}

    def test_placeholder_reference_never_rehashes(self):
        prev = version("f", "f@1", BUILD_PREV, job("a", ref="External"))
        curr = version("f", "f@2", BUILD, job("b", ref="External"))
        assert match_versions(prev, curr) == {}

    def test_ambiguous_reference_never_rehashes(self):
        prev = version("f", "f@1", BUILD_PREV, job("a", ref="req23583"), job("b", ref="req23583"))
        curr = version("f", "f@2", BUILD, job("c", ref="req23583"))
        assert match_versions(prev, curr) == {}

    def test_reference_is_scoped_to_employer(self):
        prev = version("f", "f@1", BUILD_PREV, job("a", org="jgp:1", ref="R-100"))
        curr = version("f", "f@2", BUILD, job("b", org="jgp:2", ref="R-100"))
        assert match_versions(prev, curr) == {}


class TestEmployerChecks:
    def _rows(self, prev, curr, *others, imported=None):
        latest = {**{o.source: o.version_id for o in others}, curr.source: curr.version_id}
        ctx = ctx_for(*(v for v in (prev, curr, *others) if v), latest=latest, imported=imported)
        matched = match_versions(prev, curr) if prev else {}
        return {r.org_key: r for r in employer_checks(prev, curr, matched, ctx, {})}

    def test_employer_vanishes(self):
        future = datetime(2026, 10, 20, tzinfo=UTC)
        prev = version("f", "f@1", BUILD_PREV, *(job(f"s{i}", close=future) for i in range(6)),
                       job("k1", org="jgp:560", name="Kent County Council"))
        curr = version("f", "f@2", BUILD, job("k1", org="jgp:560", name="Kent County Council"))
        surrey = self._rows(prev, curr)["jgp:116"]
        assert (surrey.still_showing, surrey.jobs_prev, surrey.jobs_now) == (False, 6, 0)
        assert (surrey.removed_unexpected, surrey.expired, surrey.added) == (6, 0, 0)
        assert self._rows(prev, curr)["jgp:560"].still_showing

    def test_counts_add_up(self):
        past, future = datetime(2026, 9, 20, tzinfo=UTC), datetime(2026, 10, 20, tzinfo=UTC)
        prev = version("f", "f@1", BUILD_PREV, job("keep"), job("exp", close=past), job("early", close=future),
                       job("nodate"), job("old-hash", ref="R-7"))
        curr = version("f", "f@2", BUILD, job("keep"), job("new1"), job("new-hash", ref="R-7"))
        r = self._rows(prev, curr)["jgp:116"]
        assert (r.jobs_prev, r.jobs_now, r.added, r.rehashed) == (5, 3, 1, 1)
        assert (r.expired, r.removed_unexpected, r.removed_no_close, r.moved) == (1, 1, 1, 0)

    def test_first_version_has_no_previous(self):
        curr = version("tribepad", "t@1", BUILD, job("a"), job("b"))
        r = self._rows(None, curr)["jgp:116"]
        assert (r.jobs_prev, r.jobs_now, r.added, r.still_showing) == (None, 2, None, True)

    def test_valid_empty_feed_then_recovery(self):
        future = datetime(2026, 10, 20, tzinfo=UTC)
        v1 = version("f", "f@1", BUILD_PREV, job("a", close=future), job("b", close=future))
        v2 = version("f", "f@2", BUILD)
        emptied = self._rows(v1, v2)["jgp:116"]
        assert (emptied.still_showing, emptied.removed_unexpected) == (False, 2)
        v3 = version("f", "f@3", BUILD + timedelta(hours=3), job("a", close=future), job("b", close=future))
        back = self._rows(v2, v3)["jgp:116"]
        assert (back.still_showing, back.jobs_prev, back.added) == (True, 0, 2)


class TestClusters:
    def test_empty_urls_never_link(self):
        a = version("ats", "a@1", BUILD, job("x1", url=""), job("x2", url=""))
        c = Clusters([a])
        assert c.root(("ats", "a@1", "x1")) != c.root(("ats", "a@1", "x2"))

    def test_url_shared_within_a_version_does_not_link_across_feeds(self):
        ats = version("ats", "a@1", BUILD, job("x1", url="https://c.example/careers"), job("x2", url="https://c.example/careers"))
        v1 = version("v1", "v@1", BUILD, job("y1", url="https://c.example/careers"))
        c = Clusters([ats, v1])
        assert c.root(("v1", "v@1", "y1")) not in {c.root(("ats", "a@1", "x1")), c.root(("ats", "a@1", "x2"))}

    def test_unique_url_links_across_feeds(self):
        c = Clusters([version("ats", "a@1", BUILD, job("x1", url="https://c.example/j/1")),
                      version("v1", "v@1", BUILD, job("y1", url="https://c.example/j/1"))])
        assert c.root(("ats", "a@1", "x1")) == c.root(("v1", "v@1", "y1"))

    def test_same_version_conflict_is_flagged(self):
        x = version("x", "x@1", BUILD, job("A", url="https://u.example/1"), job("C", org="jgp:1", ref="R-1"))
        y = version("y", "y@1", BUILD, job("B", org="jgp:1", ref="R-1", url="https://u.example/1"))
        assert Clusters([x, y]).same_version_conflicts() == 1


class TestNotReaching:
    def _gaps(self, *jobs, export=(), first_seen=None, others=()):
        v = version("f", "f@1", BUILD, *jobs, first_seen=first_seen or {})
        clusters = Clusters([v, *others])
        return [g.job_id for g in not_reaching([v], clusters, frozenset(export), RUN, timedelta(hours=5), {})]

    def test_live_job_reaches(self):
        assert self._gaps(job("a"), export={"a"}) == []

    def test_missing_after_grace_is_a_gap(self):
        assert self._gaps(job("a")) == ["a"]  # first seen = build 06:55, run 12:00: 5h05m

    def test_new_job_within_grace_is_not_yet_a_gap(self):
        assert self._gaps(job("a"), first_seen={"a": RUN - timedelta(hours=2)}) == []

    def test_job_past_its_close_date_is_not_a_gap(self):
        assert self._gaps(job("a", close=RUN - timedelta(hours=1))) == []

    def test_twin_live_on_jobiqo_counts_as_reaching(self):
        twin = version("g", "g@1", BUILD, job("b", url="https://x.example/j9"))
        assert self._gaps(job("a", url="https://x.example/j9"), export={"b"}, others=[twin]) == []


class TestExportProblem:
    def test_ok(self):
        assert export_problem(7800, 7700, RUN - timedelta(hours=1), RUN, 0.2, timedelta(hours=12)) is None

    def test_shrank(self):
        assert "shrank" in export_problem(5000, 7800, RUN, RUN, 0.2, timedelta(hours=12))

    def test_stale(self):
        assert "not updated" in export_problem(7800, 7800, RUN - timedelta(hours=20), RUN, 0.2, timedelta(hours=12))

    def test_empty(self):
        assert export_problem(0, None, RUN, RUN, 0.2, timedelta(hours=12)) == "export has no jobs"


class TestSummaries:
    def test_feed_summary(self):
        prev = version("f", "f@1", BUILD_PREV, job("a"), job("b"), job("k", org="jgp:560", name="Kent"))
        curr = version("f", "f@2", BUILD, job("a"), job("c", org="jgp:34", name="Medway"))
        s = feed_summary(prev, curr, match_versions(prev, curr))
        assert (s.employers_total, s.employers_added, s.employers_removed) == (2, 1, 1)
        assert (s.vacancies_total, s.vacancies_added, s.vacancies_removed) == (2, 1, 2)

    def test_first_version_summary_has_no_deltas(self):
        s = feed_summary(None, version("f", "f@1", BUILD, job("a")), None)
        assert (s.vacancies_total, s.vacancies_added, s.employers_added) == (1, None, None)

    def test_move_between_feeds_nets_to_zero_in_all(self):
        a_prev = version("a", "a@1", BUILD_PREV, job("x", url="https://j.example/1"), job("keep", org="jgp:9", name="Nine"))
        a_now = version("a", "a@2", BUILD, job("keep", org="jgp:9", name="Nine"))
        b_prev = version("b", "b@1", BUILD_PREV)
        b_now = version("b", "b@2", BUILD, job("y", url="https://j.example/1"))
        c = Clusters([a_prev, a_now, b_prev, b_now])
        s = all_feeds_summary([a_now, b_now], [a_prev, b_prev], c, {})
        assert (s.vacancies_total, s.vacancies_added, s.vacancies_removed) == (2, 0, 0)
        assert (s.employers_total, s.employers_added, s.employers_removed) == (2, 0, 0)

    def test_employer_names_merge_namespaces(self):
        jgp = version("ats", "a@1", BUILD, job("x", org="jgp:77", name="HM Revenue & Customs"))
        cs = version("cs", "c@1", BUILD, job("y", org="cs:4600", name=""))
        export = JobiqoExport(ids=frozenset({"y"}), company_by_id={"y": "HM Revenue & Customs"}, jobs=1)
        names = logic.employer_names([jgp, cs], export)
        s = all_feeds_summary([jgp, cs], [jgp, cs], Clusters([jgp, cs]), names)
        assert names["cs:4600"] == "HM Revenue & Customs" and s.employers_total == 1


def test_first_seen_carries_forward_including_rehash():
    early = datetime(2026, 9, 1, tzinfo=UTC)
    prev = version("f", "f@1", BUILD_PREV, job("a"), job("old", ref="R-5"), first_seen={"a": early, "old": early})
    curr = version("f", "f@2", BUILD, job("a"), job("new", ref="R-5"), job("fresh"))
    first = carry_first_seen(prev, curr.jobs, curr.build_ts, match_versions(prev, curr))
    assert first == {"a": early, "new": early, "fresh": BUILD}


def test_names_fallback_for_unnamed_civil_service_dept():
    assert logic.name_for("cs:4204", {}) == "CS dept 4204"


class TestLiveAndDuplicates:
    def test_live_counts_own_id_only(self):
        ats = version("ats", "a@1", BUILD, job("x1", url="https://j.example/1"), job("x2"), job("x3", org="jgp:9", name="Nine"))
        v1 = version("v1", "v@1", BUILD, job("y1", url="https://j.example/1"))
        live = logic.live_listings([ats, v1], frozenset({"x1", "x3"}))
        assert live.by_source == {"ats": 2, "v1": 0}   # y1's twin x1 is live, but y1 itself isn't
        assert live.by_employer == {("ats", "jgp:116"): 1, ("ats", "jgp:9"): 1, ("v1", "jgp:116"): 0}

    def test_duplicate_groups_basis_and_live_copies(self):
        ats = version("ats", "a@1", BUILD, job("x1", url="https://j.example/1", title="Social Worker"),
                      job("x2", ref="COV/77", title="Care Assistant"), job("solo"))
        v1 = version("v1", "v@1", BUILD, job("y1", url="https://j.example/1", title="Social Worker"),
                     job("y2", ref="COV/77", title="Care Assistant"))
        groups = logic.duplicate_groups([ats, v1], Clusters([ats, v1]), frozenset({"x1", "y1", "x2"}), {})
        assert [(g.basis, [m.source for m in g.members], [m.live for m in g.members]) for g in groups] == [
            ("same reference", ["ats", "v1"], [True, False]),   # Care Assistant sorts first
            ("same URL", ["ats", "v1"], [True, True]),
        ]
        assert len({g.group_id for g in groups}) == 2

    def test_same_id_in_two_feeds_is_a_duplicate(self):
        scrape = version("scrape", "s@1", BUILD, job("h1"))
        tribepad = version("tribepad", "t@1", BUILD, job("h1"))
        (g,) = logic.duplicate_groups([scrape, tribepad], Clusters([scrape, tribepad]), None, {})
        assert g.basis == "same ID" and all(m.live is None for m in g.members)   # no trusted export: unknown

    def test_two_jobs_of_one_feed_are_not_a_cross_feed_duplicate(self):
        ats = version("ats", "a@1", BUILD, job("x1", url="https://j.example/1"), job("x2", ref="R-1"))
        assert logic.duplicate_groups([ats], Clusters([ats]), frozenset(), {}) == []
