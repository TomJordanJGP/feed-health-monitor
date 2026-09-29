"""Headless tests for the dashboard's display logic. Every table is also serialised with
pyarrow, the path st.dataframe takes, because a column that pandas holds happily can still
render blank in Streamlit (JPD lesson, 2026-07-09)."""
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pytest

from dashboard import helpers

UTC = timezone.utc


def ts(*args):
    return pd.Timestamp(datetime(*args, tzinfo=UTC))


class TestTimes:
    @pytest.mark.parametrize("utc, uk", [
        (ts(2026, 9, 29, 13, 16), "Tue 29 Sep, 14:16"),     # BST
        (ts(2026, 10, 25, 0, 30), "Sun 25 Oct, 01:30"),     # last half hour of BST
        (ts(2026, 10, 25, 12, 0), "Sun 25 Oct, 12:00"),     # GMT after the clocks go back
    ])
    def test_uk_time_across_the_end_of_bst(self, utc, uk):
        assert helpers.uk_time(utc) == uk

    @pytest.mark.parametrize("value", [None, pd.NaT, float("nan")])
    def test_missing_time_is_a_dash(self, value):
        assert helpers.uk_time(value) == helpers.DASH

    @pytest.mark.parametrize("hours, label", [(0.5, "30 min"), (5.26, "5.3 h"), (72, "3.0 days"), (None, "—")])
    def test_age_labels(self, hours, label):
        assert helpers.age_label(hours) == label

    @pytest.mark.parametrize("hours_ago, state", [(0.5, "green"), (2.5, "orange"), (5, "red")])
    def test_monitor_state(self, hours_ago, state):
        now = datetime(2026, 9, 29, 12, tzinfo=UTC)
        assert helpers.monitor_state(now - timedelta(hours=hours_ago), now, 2, 4) == state

    def test_no_run_at_all_is_red(self):
        assert helpers.monitor_state(None, ts(2026, 9, 29, 12), 2, 4) == "red"


def status_frame():
    rows = [
        dict(scope="civil_service_jobs_uk.xml", label="JGP scrape | Civil Service Jobs UK", show=True, configured=True,
             status="green", build_ts=ts(2026, 9, 29, 11, 48), age_hours=1.5, jobs=1307, employers=105, error=None, note=None, export_status=None),
        dict(scope="jgp_scraping_feed.xml", label="JGP scrape", show=True, configured=True,
             status="red", build_ts=ts(2026, 9, 28, 18, 2), age_hours=17.5, jobs=818, employers=45, error=None, note=None, export_status=None),
        dict(scope="jgp_tribepad_feed.xml", label="JGP Tribepad", show=False, configured=True,
             status="green", build_ts=ts(2026, 9, 29, 11, 48), age_hours=1.5, jobs=388, employers=14, error=None, note=None, export_status=None),
        dict(scope="new_feed.xml", label="new_feed.xml", show=True, configured=False,
             status="error", build_ts=pd.NaT, age_hours=None, jobs=None, employers=None, error="HTTP 404", note=None, export_status=None),
        dict(scope="jobiqo_export", label="Jobiqo export (Appcast)", show=None, configured=None,
             status="green", build_ts=ts(2026, 9, 29, 13, 2), age_hours=0.2, jobs=7915, employers=446, error=None, note=None, export_status="ok"),
        dict(scope="ALL", label="All feeds", show=None, configured=None, status=None, build_ts=pd.NaT,
             age_hours=None, jobs=8624, employers=445, error=None, note=None, export_status="ok"),
    ]
    return pd.DataFrame(rows)


def test_incoming_table_with_total_row():
    table = helpers.incoming_table(status_frame())
    assert list(table["Feed"]) == ["new_feed.xml", "JGP scrape", "JGP scrape | Civil Service Jobs UK", "Total"]
    new = table.iloc[0]      # hidden Tribepad, the export and the ALL row are not incoming rows
    assert new["Status"] == "⚠️ Error" and "HTTP 404" in new["Notes"] and "not in config" in new["Notes"]
    assert table.iloc[1]["Status"] == "🔴 Stale" and table.iloc[1]["Last built"] == "Mon 28 Sep, 19:02"
    total = table.iloc[-1]
    assert (total["Jobs"], total["Employers"]) == ("2,125", "150")    # 818 + 1,307 and 45 + 105
    pa.Table.from_pandas(table)


def test_outgoing_table_counts_export_employers():
    table = helpers.outgoing_table(status_frame())
    assert list(table["Feed"]) == ["Jobiqo export (what's live on the board)", "Total"]
    assert (table.iloc[0]["Employers"], table.iloc[1]["Jobs"]) == ("446", "7,915")
    pa.Table.from_pandas(table)


def summary_frame():
    return pd.DataFrame([
        dict(scope="jgp_ats_feed.xml", label="JGP ATS feed", employers_total=167, employers_added=1, employers_removed=0,
             vacancies_total=825, vacancies_added=9, vacancies_removed=4, live=808, not_reaching=2, duplicates=None,
             prev_version_ts=ts(2026, 9, 29, 10, 17)),
        dict(scope="jgp_backfill_feed_v1.xml", label="JGP scrape | JGP Backfill", employers_total=33, employers_added=None,
             employers_removed=None, vacancies_total=704, vacancies_added=None, vacancies_removed=None, live=704,
             not_reaching=0, duplicates=None, prev_version_ts=pd.NaT),
        dict(scope="ALL", label="All feeds", employers_total=190, employers_added=0, employers_removed=0,
             vacancies_total=1446, vacancies_added=0, vacancies_removed=0, live=1512, not_reaching=2, duplicates=83,
             prev_version_ts=pd.NaT),
    ])


def test_summary_table_has_no_all_row_and_a_plain_total():
    table = helpers.summary_table(summary_frame())
    assert list(table["Feed"]) == ["JGP ATS feed", "JGP scrape | JGP Backfill", "Total"]
    total = table.iloc[-1]
    assert (total["Employers"], total["Vacancies"], total["Live on Jobiqo"]) == ("200", "1,529", "1,512")
    assert (total["Vacancies added"], total["Not reaching Jobiqo"], total["Changes since"]) == ("9", "2", "")
    assert table.iloc[1]["Vacancies added"] == helpers.DASH and table.iloc[1]["Changes since"] == "first check"
    assert table.iloc[0]["Changes since"] == "Tue 29 Sep, 11:17"
    pa.Table.from_pandas(table)


def test_summary_totals_are_plain_sums_plus_duplicates():
    totals = helpers.summary_totals(summary_frame())
    assert (totals["employers"], totals["vacancies"], totals["live"], totals["duplicates"]) == (200, 1529, 1512, 83)


def employers_frame():
    base = dict(label="JGP scrape", show=True, jobs_prev=10, jobs_now=10, on_jobiqo=10, expired=0, removed_unexpected=0,
                removed_no_close=0, moved=0, not_reaching=0, still_showing=True, last_seen_ts=pd.NaT,
                this_check_ts=ts(2026, 9, 29, 9, 2), last_check_ts=ts(2026, 9, 28, 9, 2))
    return pd.DataFrame([
        {**base, "org_name": "Kent County Council"},
        {**base, "org_name": "Surrey County Council", "jobs_prev": 67, "jobs_now": 0, "removed_unexpected": 67,
         "still_showing": False, "last_seen_ts": ts(2026, 9, 28, 9, 2)},
        {**base, "org_name": "Medway Council", "not_reaching": 3},
        {**base, "org_name": "Hidden Tribepad Org", "label": "JGP Tribepad", "show": False, "removed_unexpected": 5},
        {**base, "org_name": "Milton Keynes Council", "label": "JGP scrape | JGP Backfill", "jobs_prev": 28,
         "jobs_now": 12, "removed_no_close": 17},
        # A feed's first version: nothing to compare yet, so the counts are NULL (pd.NA from BigQuery).
        {**base, "org_name": "Greenwich First Check", "label": "JGP scrape | JGP London", "jobs_prev": None,
         "expired": None, "removed_unexpected": None, "removed_no_close": None, "moved": None, "not_reaching": None,
         "on_jobiqo": None},
    ])


def test_issues_only_flags_vanished_early_removals_and_gaps_not_undated_removals():
    table = helpers.employers_table(employers_frame(), issues_only=True)
    assert list(table["Employer"]) == ["Surrey County Council", "Medway Council"]  # first-version NAs aren't issues
    assert table.iloc[0]["Showing"] == "❌ No (last seen Mon 28 Sep, 10:02)"
    pa.Table.from_pandas(table)


def test_all_employers_with_filters():
    table = helpers.employers_table(employers_frame(), feeds=["JGP scrape"], search="council")
    assert set(table["Employer"]) == {"Kent County Council", "Surrey County Council", "Medway Council"}
    assert helpers.employers_table(employers_frame(), search="milton").iloc[0]["Removed (no closing date)"] == 17
    cols = list(table.columns)
    assert cols.index("On Jobiqo") == cols.index("Jobs this check") + 1
    pa.Table.from_pandas(table)


def test_no_issues_gives_an_empty_table_that_still_serialises():
    frame = employers_frame().iloc[[0]]
    table = helpers.employers_table(frame, issues_only=True)
    assert table.empty
    pa.Table.from_pandas(table)


def duplicates_frame():
    rows = []
    for gid, basis, emp, title, copies in (("g1", "same URL", "Coventry City Council", "Social Worker", (True, True)),
                                            ("g2", "same reference", "Lincolnshire County Council", "Care Assistant", (True, False))):
        for (source, label), live in zip((("jgp_ats_feed.xml", "JGP ATS feed"),
                                          ("jgp_backfill_feed_v1.xml", "JGP scrape | JGP Backfill")), copies):
            rows.append(dict(group_id=gid, basis=basis, source=source, label=label, job_id=f"{gid}-{source}",
                             org_key="jgp:1", org_name=emp, title=title, live=live))
    return pd.DataFrame(rows)


def test_duplicates_table_one_row_per_vacancy():
    table = helpers.duplicates_table(duplicates_frame())
    assert list(table["Employer"]) == ["Coventry City Council", "Lincolnshire County Council"]
    assert list(table["Copies live on Jobiqo"]) == ["2 of 2", "1 of 2"]
    assert table.iloc[0]["Feeds"] == "JGP ATS feed + JGP scrape | JGP Backfill"
    assert helpers.live_twice(duplicates_frame()) == 1
    assert list(helpers.duplicates_table(duplicates_frame(), search="care")["Vacancy"]) == ["Care Assistant"]
    pa.Table.from_pandas(table)


def test_duplicates_table_empty_and_unknown_live():
    assert helpers.duplicates_table(pd.DataFrame()).empty
    unknown = duplicates_frame().assign(live=None)
    assert set(helpers.duplicates_table(unknown)["Copies live on Jobiqo"]) == {"unknown"}
    assert helpers.live_twice(unknown) is None
