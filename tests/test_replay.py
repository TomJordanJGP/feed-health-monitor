"""Replay Jobiqo's 28 historic imports through the real pipeline and check it reproduces what
the 29 Sep 2026 backtest established.

The files are the exact XMLs Jobiqo stored at each import (data/history/, gitignored; source
list in data/Feed_history.xlsx). Each file name is "<unix import time>-<feed file>". The test
is skipped when the files aren't present (e.g. in CI).
"""
from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone

import pytest

from feed_health import pipeline
from feed_health.config import PROJECT_ROOT, load_settings
from feed_health.feeds import FetchResult, parse_feed
from feed_health.pipeline import LastCheck, Observation, SourceState

HISTORY = PROJECT_ROOT / "data" / "history"
FILES = sorted(HISTORY.glob("*.xml")) if HISTORY.exists() else []
pytestmark = pytest.mark.skipif(len(FILES) != 28, reason="needs the 28 Jobiqo import files in data/history")

UNCHANGED = FetchResult(304, None, None, None, None, None)


def replay_imports():
    """Feed the imports to compute_run in import order, as the hourly collector would have seen
    them: one source changed per run, every other known source unchanged."""
    settings = load_settings(require_urls=False)
    imports = sorted((int(m.group(1)), m.group(2), p) for p in FILES
                     if (m := re.fullmatch(r"(\d{10})-(.+\.xml)", p.name)))
    states: dict[str, SourceState] = {}
    transitions = defaultdict(dict)   # (source, "MM-DD HH:MM" UTC of the new build) -> {org_key: EmployerCheck}
    for ts, source, path in imports:
        run_ts = datetime.fromtimestamp(ts, timezone.utc)
        parsed = parse_feed(path.read_bytes(), run_ts)
        fetch = FetchResult(200, None, None, str(ts), parsed.build_date, None)
        observations = [Observation(source, settings.feed(source), fetch, parsed)]
        observations += [Observation(s, settings.feed(s), UNCHANGED) for s in states if s != source]
        out = pipeline.compute_run(run_ts, settings, states, observations, export_obs=None)
        for row in out.employer_rows:
            transitions[(row.source, row.build_ts.strftime("%m-%d %H:%M"))][row.org_key] = row
        v = out.latest[source]
        states[source] = SourceState(
            latest=v, previous=out.previous[source],
            latest_hash=out.new_version_meta[source]["content_hash"] if source in out.new_versions else states[source].latest_hash,
            last_check=LastCheck(None, str(ts), parsed.build_date, parsed.build_date, parsed.build_date, 0, len(v.jobs)))
    return transitions


@pytest.fixture(scope="module")
def transitions():
    return replay_imports()


def gone(r):
    return r.expired + r.moved + r.removed_unexpected + r.removed_no_close


def by_name(rows, fragment):
    (row,) = [r for r in rows.values() if fragment in r.org_name]
    return row


def test_print_overview(transitions, capsys):
    with capsys.disabled():
        print()
        for (source, day), rows in sorted(transitions.items()):
            firsts = sum(r.jobs_prev is None for r in rows.values())
            if firsts == len(rows):
                print(f"  {source:42s} {day} first version ({sum(r.jobs_now for r in rows.values())} jobs)")
                continue
            tot = lambda f: sum(getattr(r, f) or 0 for r in rows.values())
            print(f"  {source:42s} {day} prev {tot('jobs_prev'):5d} now {tot('jobs_now'):5d} added {tot('added'):4d} "
                  f"expired {tot('expired'):4d} moved {tot('moved'):3d} unexpected {tot('removed_unexpected'):3d} "
                  f"no-close {tot('removed_no_close'):3d} rehashed {tot('rehashed'):3d}")


# Facts established independently by the 29 Sep backtest (a separate prototype over the same
# 28 files). Grace = 24h (config). Keys are (source, "MM-DD HH:MM" UTC of the new build).
V1_SUNDAY = ("jgp_backfill_feed_v1.xml", "09-28 06:55")


def totals(transitions, source):
    rows = [r for (s, _), t in transitions.items() if s == source for r in t.values() if r.jobs_prev is not None]
    return sum(gone(r) for r in rows), sum(r.expired for r in rows)


@pytest.mark.parametrize("source, gone_total, expired", [
    ("civil_service_jobs_uk.xml", 172, 171),
    ("jgp_backfill_feed_v3.xml", 469, 469),
    ("jgp_scraping_feed.xml", 106, 100),
    ("jgp_backfill_feed_v2.xml", 87, 61),
    ("jgp_ats_feed.xml", 25, 25),
    ("jgp_falkland_islands_government_feed.xml", 2, 2),
    # V1 has no close dates of its own: 30 are resolved by borrowing Organic-1's (see Lincolnshire).
    ("jgp_backfill_feed_v1.xml", 158, 30),
])
def test_closing_dates_explain_removals_as_in_backtest(transitions, source, gone_total, expired):
    assert totals(transitions, source) == (gone_total, expired)


def test_only_three_jobs_removed_unexpectedly(transitions):
    """Backtest: only 3 jobs vanished more than 24h before closing: 1 Civil Service, 2 London."""
    found = sorted((s, day, r.removed_unexpected) for (s, day), t in transitions.items()
                   for r in t.values() if r.removed_unexpected)
    assert found == [("civil_service_jobs_uk.xml", "09-26 06:17", 1),
                     ("jgp_backfill_feed_v2.xml", "09-26 06:17", 1),
                     ("jgp_backfill_feed_v2.xml", "09-27 07:48", 1)]


def test_milton_keynes_weekend_drop(transitions):
    """The backtest's one sharp-drop firing: 28 -> 12, 17 gone, 1 new; V1 has no close dates."""
    mk = transitions[V1_SUNDAY]["jgp:322"]
    assert (mk.jobs_prev, mk.jobs_now, mk.added, mk.removed_no_close, mk.expired) == (28, 12, 1, 17, 0)


def test_lincolnshire_borrows_close_dates_from_organic(transitions):
    """76 -> 47: all 30 lost jobs are the same (org, reference) vacancies as in Organic-1,
    whose close dates had passed, so they are expired rather than unexplained."""
    lincs = by_name(transitions[V1_SUNDAY], "Lincolnshire")
    assert (lincs.jobs_prev, lincs.jobs_now, lincs.expired, lincs.removed_no_close) == (76, 47, 30, 0)


def test_organic_new_batch_of_employers(transitions):
    """28 Sep 10:44: the batch reviewed in the Test feed went live: 397 jobs, 12 new employers."""
    rows = transitions[("jgp_backfill_feed_v3.xml", "09-28 10:44")].values()
    assert sum(r.added for r in rows) == 397
    assert sum(1 for r in rows if r.jobs_prev == 0 and r.jobs_now > 0) == 12


def test_url_change_is_a_rehash_not_a_loss(transitions):
    assert sum(r.rehashed for r in transitions[("jgp_backfill_feed_v2.xml", "09-26 06:17")].values()) == 1


def test_nothing_misread_as_moved(transitions):
    assert sum(r.moved or 0 for t in transitions.values() for r in t.values()) == 0
