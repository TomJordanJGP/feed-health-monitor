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
        dict(scope="civil_service_jobs_uk.xml", label="JGP scrape | Civil Service Jobs UK", show=True, imported=True, configured=True,
             status="green", build_ts=ts(2026, 9, 29, 11, 48), age_hours=1.5, jobs=1307, employers=105, error=None, note=None, export_status=None),
        dict(scope="jgp_scraping_feed.xml", label="JGP scrape", show=True, imported=True, configured=True,
             status="red", build_ts=ts(2026, 9, 28, 18, 2), age_hours=17.5, jobs=818, employers=45, error=None, note=None, export_status=None),
        dict(scope="jgp_tribepad_feed.xml", label="JGP Tribepad", show=True, imported=False, configured=True,
             status="green", build_ts=ts(2026, 9, 29, 11, 48), age_hours=1.5, jobs=388, employers=14, error=None, note=None, export_status=None),
        dict(scope="new_feed.xml", label="new_feed.xml", show=True, imported=True, configured=False,
             status="error", build_ts=pd.NaT, age_hours=None, jobs=None, employers=None, error="HTTP 404", note=None, export_status=None),
        dict(scope="jobiqo_export", label="Jobiqo export (Appcast)", show=None, imported=None, configured=None,
             status="green", build_ts=ts(2026, 9, 29, 13, 2), age_hours=0.2, jobs=7915, employers=446, error=None, note=None, export_status="ok"),
        dict(scope="ALL", label="All feeds", show=None, imported=None, configured=None, status=None, build_ts=pd.NaT,
             age_hours=None, jobs=8624, employers=445, error=None, note=None, export_status="ok"),
    ]
    return pd.DataFrame(rows)


def test_incoming_table_with_total_row():
    table = helpers.incoming_table(status_frame())
    assert list(table["Feed"]) == ["new_feed.xml", "JGP scrape", "JGP scrape | Civil Service Jobs UK", "JGP Tribepad",
                                   "Total"]   # the export and the ALL row are not incoming rows
    new = table.iloc[0]
    assert new["Status"] == "⚠️ Error" and "HTTP 404" in new["Notes"] and "not in config" in new["Notes"]
    assert "not imported" not in new["Notes"]
    assert table.iloc[1]["Status"] == "🔴 Stale" and table.iloc[1]["Last built"] == "Mon 28 Sep, 19:02"
    tribepad = table.iloc[3]
    assert (tribepad["Jobs"], tribepad["Notes"]) == ("388", "not imported by Jobiqo")
    total = table.iloc[-1]
    assert (total["Jobs"], total["Employers"]) == ("2,125", "150")    # 818 + 1,307 and 45 + 105: no Tribepad
    pa.Table.from_pandas(table)


def test_hidden_feed_is_not_listed():
    status = status_frame()
    status.loc[status["scope"] == "jgp_tribepad_feed.xml", "show"] = False
    assert "JGP Tribepad" not in set(helpers.incoming_table(status)["Feed"])


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
                removed_no_close=0, moved=0, not_reaching=0, not_imported=None, still_showing=True, last_seen_ts=pd.NaT,
                this_check_ts=ts(2026, 9, 29, 9, 2), last_check_ts=ts(2026, 9, 28, 9, 2))
    return pd.DataFrame([
        {**base, "org_name": "Kent County Council"},
        {**base, "org_name": "Surrey County Council", "jobs_prev": 67, "jobs_now": 0, "removed_unexpected": 67,
         "still_showing": False, "last_seen_ts": ts(2026, 9, 28, 9, 2)},
        {**base, "org_name": "Medway Council", "not_reaching": 3},
        {**base, "org_name": "Hidden Org", "label": "JGP Hidden", "show": False, "removed_unexpected": 5},
        # A feed Jobiqo doesn't import: its missing jobs are "not imported", which isn't an issue.
        {**base, "org_name": "Tribepad Only Trust", "label": "JGP Tribepad", "on_jobiqo": 4, "not_reaching": None,
         "not_imported": 6},
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


def test_not_imported_is_shown_but_not_an_issue():
    table = helpers.employers_table(employers_frame(), feeds=["JGP Tribepad"])
    (r,) = table.to_dict("records")
    assert (r["Employer"], r["Not imported"], r["On Jobiqo"]) == ("Tribepad Only Trust", 6, 4)
    assert pd.isna(r["Not reaching Jobiqo"])
    assert helpers.employers_table(employers_frame(), feeds=["JGP Tribepad"], issues_only=True).empty
    cols = list(table.columns)
    assert cols.index("Not imported") == cols.index("Not reaching Jobiqo") + 1
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


def companies_frame():
    import numpy as np  # BigQuery's REPEATED columns arrive as numpy arrays
    base = dict(jobiqo_id=None, ats_org_id=None, lifecycle_stage="customer", match_status="agree", matched_by="jobiqo_id",
                matched_org_keys=np.array([], dtype=object), feed_name=None, duplicate_on=None,
                duplicate_ids=np.array([], dtype=object), notes=None, feed_jobs=0, jobiqo_jobs=0,
                last_in_feed_ts=pd.NaT, jobs_status="in_feeds")
    return pd.DataFrame([
        {**base, "hubspot_id": "1", "name": "Sefton Council", "jobiqo_id": "934", "matched_org_keys": np.array(["jgp:934"]),
         "feed_name": "Sefton Council", "feed_jobs": 29, "jobiqo_jobs": 29},
        {**base, "hubspot_id": "2", "name": "Kent County Council", "match_status": "conflict", "ats_org_id": "901",
         "matched_org_keys": np.array(["jgp:300"]), "feed_name": "Medway Council", "feed_jobs": 5,
         "notes": "name points to Kent County Council"},
        {**base, "hubspot_id": "3", "name": "Surrey County Council", "match_status": "name_only",
         "matched_org_keys": np.array(["jgp:100"]), "feed_jobs": 40, "lifecycle_stage": None},
        {**base, "hubspot_id": "4", "name": "Old Customer Trust", "match_status": "no_match", "jobs_status": "never_seen"},
        {**base, "hubspot_id": "5", "name": "Sefton Council", "match_status": "name_only", "jobiqo_id": "777",
         "matched_org_keys": np.array(["jgp:934"]), "duplicate_on": "name", "duplicate_ids": np.array(["1"]),
         "lifecycle_stage": "opportunity", "feed_jobs": 29},
        {**base, "hubspot_id": "6", "name": "Lapsed Borough", "jobs_status": "last_seen",
         "last_in_feed_ts": ts(2026, 10, 12, 9, 0), "matched_org_keys": np.array(["jgp:55"])},
        {**base, "hubspot_id": "7", "name": "Prospect Ltd", "match_status": "no_match", "jobs_status": "never_seen",
         "lifecycle_stage": "lead"},
    ])


def test_companies_clean_up_items_come_first_in_order():
    table = helpers.companies_table(companies_frame())
    assert list(table["HubSpot ID"]) == ["2", "5", "3", "6", "4", "1", "7"]   # ties: most jobs, then name
    todo = dict(zip(table["HubSpot ID"], table["To do"]))
    assert todo == {"2": "Keys disagree", "5": "Duplicate; Check Jobiqo ID", "3": "Add Jobiqo ID",
                    "4": "Customer with no jobs", "6": "Customer with no jobs", "1": "", "7": ""}
    row = table.set_index("HubSpot ID").loc["5"]
    assert (row["Same key as"], row["Stage"], row["Match"]) == ("name: 1", "Opportunity", "🔤 Name only")
    assert table.set_index("HubSpot ID").loc["6", "Jobs"] == "Not now (last in a feed Mon 12 Oct)"
    assert table.set_index("HubSpot ID").loc["3", "Stage"] == helpers.NO_STAGE
    assert helpers.stage_options(companies_frame()) == ["Customer", "Opportunity", "Lead", helpers.NO_STAGE]
    pa.Table.from_pandas(table)


def test_companies_filters_and_search():
    frame = companies_frame()
    assert list(helpers.companies_table(frame, cleanup_only=True)["HubSpot ID"]) == ["2", "5", "3", "6", "4"]
    assert set(helpers.companies_table(frame, stages=["Customer"])["HubSpot ID"]) == {"1", "2", "4", "6"}
    assert list(helpers.companies_table(frame, matches=["⚠️ Keys disagree"])["HubSpot ID"]) == ["2"]
    assert set(helpers.companies_table(frame, jobs=["None seen"])["HubSpot ID"]) == {"4", "7"}
    assert set(helpers.companies_table(frame, search="sefton")["HubSpot ID"]) == {"1", "5"}
    assert list(helpers.companies_table(frame, search="901")["HubSpot ID"]) == ["2"]     # by ATS ID too
    assert list(helpers.companies_table(frame, search="medway")["HubSpot ID"]) == ["2"]  # and the feed's name
    assert helpers.companies_table(pd.DataFrame()).empty


def employers_for_hubspot():
    return pd.DataFrame([
        dict(org_key="jgp:934", org_name="Sefton Council", label="JGP ATS feed", show=True, jobs_now=29, on_jobiqo=29),
        dict(org_key="cs:4204", org_name="HM Prison and Probation Service", label="JGP scrape | Civil Service Jobs UK",
             show=True, jobs_now=40, on_jobiqo=40),
        dict(org_key="jgp:88", org_name="Unknown Trust", label="JGP ATS feed", show=True, jobs_now=3, on_jobiqo=2),
        dict(org_key="jgp:88", org_name="Unknown Trust", label="JGP scrape", show=True, jobs_now=2, on_jobiqo=None),
        dict(org_key="jgp:99", org_name="Gone Away Council", label="JGP scrape", show=True, jobs_now=0, on_jobiqo=None),
        dict(org_key="jgp:77", org_name="Hidden Feed Org", label="JGP Hidden", show=False, jobs_now=5, on_jobiqo=5),
    ])


def test_feed_employers_not_in_hubspot():
    table = helpers.employers_not_in_hubspot(employers_for_hubspot(), companies_frame())
    assert list(table["Employer"]) == ["HM Prison and Probation Service", "Unknown Trust"]   # Sefton is matched
    trust = table.iloc[1]
    assert (trust["Jobiqo ID"], trust["Feeds"], trust["Jobs now"], trust["On Jobiqo"]) == \
        ("88", "JGP ATS feed, JGP scrape", 5, 2)
    assert list(helpers.employers_not_in_hubspot(employers_for_hubspot(), companies_frame(), "trust")["Employer"]) == \
        ["Unknown Trust"]
    pa.Table.from_pandas(table)


def test_companies_totals():
    totals = helpers.companies_totals(companies_frame(), employers_for_hubspot())
    assert totals == {"companies": 7, "customers": 4, "customers_no_jobs": 2, "cleanup": 3, "employers_not_in_hubspot": 2}
