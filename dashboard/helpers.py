"""Pure display logic for the dashboard: UK times, labels, and the tables. No Streamlit import,
so it can be tested headless (including the pyarrow serialisation st.dataframe uses)."""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd

LONDON = ZoneInfo("Europe/London")
DASH = "—"
TOTAL = "Total"

STATUS_LABELS = {"green": "🟢 OK", "orange": "🟠 Ageing", "red": "🔴 Stale", "error": "⚠️ Error", "unknown": "⚪ Unknown"}


def uk_time(ts) -> str:
    """'Tue 29 Sep, 14:16' in UK time (BST or GMT as applicable); a dash when missing."""
    if ts is None or pd.isna(ts):
        return DASH
    ts = pd.Timestamp(ts)
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    return ts.tz_convert(LONDON).strftime("%a %d %b, %H:%M")


def age_label(hours) -> str:
    if hours is None or pd.isna(hours):
        return DASH
    if hours < 1:
        return f"{round(hours * 60)} min"
    if hours < 48:
        return f"{hours:.1f} h"
    return f"{hours / 24:.1f} days"


def status_label(status) -> str:
    return STATUS_LABELS.get(status, DASH) if isinstance(status, str) else DASH


def monitor_state(last_run: datetime | None, now: datetime, orange_hours: float, red_hours: float) -> str:
    """How fresh the monitor's own data is: a dead monitor must not leave the page looking green."""
    if last_run is None or pd.isna(last_run):
        return "red"
    hours = (pd.Timestamp(now) - pd.Timestamp(last_run)).total_seconds() / 3600
    return "red" if hours > red_hours else "orange" if hours > orange_hours else "green"


def truthy(series: pd.Series) -> pd.Series:
    """True only where the value is True: None/NA count as False, for object or nullable dtypes."""
    return series.eq(True).fillna(False).astype(bool)


def _int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _count_text(series: pd.Series) -> pd.Series:
    """Counts as display text ('8,624', or a dash where there is nothing to compare). Used for
    small tables: Streamlit renders numeric NA as 'None' whatever the formatting."""
    return _int(series).map(lambda n: DASH if pd.isna(n) else f"{int(n):,}")


def column_total(series: pd.Series):
    """A plain sum; NA only when every value is NA (e.g. nothing to compare on a first check)."""
    return _int(series).sum(min_count=1)


def _with_total(table: pd.DataFrame, first_col: str, sums: dict[str, pd.Series]) -> pd.DataFrame:
    """Append a Total row that simply sums the given (numeric) source columns."""
    total = dict.fromkeys(table.columns, "")
    total[first_col] = TOTAL
    for col, series in sums.items():
        total[col] = _count_text(pd.Series([column_total(series)])).iloc[0]
    return pd.DataFrame([*table.to_dict("records"), total], columns=table.columns)


# --------------------------------------------------------------------------- feed status

def _status_rows(rows: pd.DataFrame) -> pd.DataFrame:
    def note(r) -> str:
        parts = [r["error"]] if isinstance(r["error"], str) else []
        if isinstance(r.get("note"), str):
            parts.append(r["note"])
        if r["scope"].endswith(".xml") and not bool(r["imported"]):
            parts.append("not imported by Jobiqo")
        if r["scope"].endswith(".xml") and not bool(r["configured"]):
            parts.append("not in config/feeds.yml")
        if r["scope"] == "jobiqo_export" and isinstance(r.get("export_status"), str) and r["export_status"] != "ok":
            parts.append(r["export_status"])
        return "; ".join(parts)

    return pd.DataFrame({
        "Feed": rows["label"],
        "Status": rows["status"].map(status_label),
        "Last built": rows["build_ts"].map(uk_time),
        "Age": rows["age_hours"].map(age_label),
        "Jobs": _count_text(rows["jobs"]),
        "Employers": _count_text(rows["employers"]),
        "Notes": rows.apply(note, axis=1) if len(rows) else pd.Series(dtype="object"),
    })


def incoming_table(status: pd.DataFrame) -> pd.DataFrame:
    """The shown feeds on the feed map, stalest first. A feed Jobiqo doesn't import (Tribepad)
    is listed with a note but left out of the Total, which counts what Jobiqo takes in."""
    rows = status[status["scope"].str.endswith(".xml") & truthy(status["show"])]
    rows = rows.sort_values("age_hours", ascending=False, na_position="first")
    imported = rows[truthy(rows["imported"])]
    return _with_total(_status_rows(rows), "Feed", {"Jobs": imported["jobs"], "Employers": imported["employers"]})


def outgoing_table(status: pd.DataFrame) -> pd.DataFrame:
    """What Jobiqo publishes: its Appcast export (employers = distinct Jobiqo organisations)."""
    rows = status[status["scope"] == "jobiqo_export"].copy()
    rows["label"] = "Jobiqo export (what's live on the board)"
    return _with_total(_status_rows(rows), "Feed", {"Jobs": rows["jobs"], "Employers": rows["employers"]})


# --------------------------------------------------------------------------- summary

_SUMMARY_COUNTS = {
    "Employers": "employers_total", "Employers added": "employers_added", "Employers removed": "employers_removed",
    "Vacancies": "vacancies_total", "Vacancies added": "vacancies_added", "Vacancies removed": "vacancies_removed",
    "Live on Jobiqo": "live", "Not reaching Jobiqo": "not_reaching",
}


def summary_table(summary: pd.DataFrame) -> pd.DataFrame:
    """One row per feed plus a Total row that is a plain sum of each column (a vacancy or
    employer carried by two feeds counts in both, as it does in each feed's own row)."""
    rows = summary[summary["scope"] != "ALL"].sort_values("label")
    table = pd.DataFrame({"Feed": rows["label"], **{col: _count_text(rows[src]) for col, src in _SUMMARY_COUNTS.items()}})
    # A feed's first version has nothing before it to compare with.
    table["Changes since"] = [("first check" if pd.isna(ts) else uk_time(ts)) for ts in rows["prev_version_ts"]]
    return _with_total(table, "Feed", {col: rows[src] for col, src in _SUMMARY_COUNTS.items()})


def summary_totals(summary: pd.DataFrame) -> dict:
    """The pull-out numbers: plain per-feed totals, plus the all-feeds duplicate count."""
    rows = summary[summary["scope"] != "ALL"]
    all_row = summary[summary["scope"] == "ALL"]
    totals = {key: column_total(rows[src]) for key, src in (
        ("employers", "employers_total"), ("vacancies", "vacancies_total"),
        ("live", "live"), ("not_reaching", "not_reaching"))}
    totals["duplicates"] = column_total(all_row["duplicates"]) if not all_row.empty else pd.NA
    return totals


# --------------------------------------------------------------------------- employers

def issue_mask(rows: pd.DataFrame) -> pd.Series:
    """An employer needs a look if it stopped showing, lost jobs before their closing dates, or
    has jobs that aren't reaching Jobiqo. Removals with no closing date are shown but not
    flagged: Backfill V1 has no closing dates at all, so they happen every day. Nor are jobs in
    a feed Jobiqo doesn't import (not_imported): those are expected to be missing. Counts are NA
    on a feed's first version (nothing to compare), which is not an issue."""
    return (~truthy(rows["still_showing"])
            | rows["removed_unexpected"].fillna(0).gt(0)
            | rows["not_reaching"].fillna(0).gt(0)).astype(bool)


def employers_table(employers: pd.DataFrame, feeds: list[str] | None = None, search: str = "",
                    issues_only: bool = False) -> pd.DataFrame:
    rows = employers[truthy(employers["show"])].copy()
    for col in ("jobs_prev", "jobs_now", "on_jobiqo", "expired", "removed_unexpected", "removed_no_close", "moved",
                "not_reaching", "not_imported"):
        rows[col] = _int(rows[col])
    if feeds:
        rows = rows[rows["label"].isin(feeds)]
    if search:
        rows = rows[rows["org_name"].str.contains(search, case=False, na=False, regex=False)]
    rows["still_showing"] = truthy(rows["still_showing"])
    rows["_issue"] = issue_mask(rows)
    if issues_only:
        rows = rows[rows["_issue"]]
    rows = rows.sort_values(
        ["_issue", "still_showing", "removed_unexpected", "not_reaching", "jobs_now"],
        ascending=[False, True, False, False, False], na_position="last")

    def showing(r) -> str:
        return "✅ Yes" if bool(r["still_showing"]) else f"❌ No (last seen {uk_time(r['last_seen_ts'])})"

    return pd.DataFrame({
        "Employer": rows["org_name"],
        "Feed": rows["label"],
        "Showing": rows.apply(showing, axis=1) if len(rows) else pd.Series(dtype="object"),
        "Jobs last check": rows["jobs_prev"],
        "Jobs this check": rows["jobs_now"],
        "On Jobiqo": rows["on_jobiqo"],
        "Expired (expected)": rows["expired"],
        "Removed (unexpected)": rows["removed_unexpected"],
        "Removed (no closing date)": rows["removed_no_close"],
        "Moved to another feed": rows["moved"],
        "Not reaching Jobiqo": rows["not_reaching"],
        "Not imported": rows["not_imported"],
        "This check": rows["this_check_ts"].map(uk_time),
        "Last check": rows.apply(lambda r: uk_time(r["last_check_ts"]) if not pd.isna(r["last_check_ts"])
                                 else ("first check" if r["still_showing"] else DASH), axis=1)
                      if len(rows) else pd.Series(dtype="object"),
    })


# --------------------------------------------------------------------------- duplicates

def duplicates_table(dups: pd.DataFrame, feeds: list[str] | None = None, search: str = "") -> pd.DataFrame:
    """One row per vacancy listed in two or more feeds, with how the copies were matched and how
    many of them are live on Jobiqo (each live copy is a separate advert on the board)."""
    columns = ["Employer", "Vacancy", "Feeds", "Matched on", "Copies live on Jobiqo"]
    if dups.empty:
        return pd.DataFrame(columns=columns)
    rows = []
    for _, g in dups.groupby("group_id", sort=False):
        labels = sorted(g["label"].fillna(g["source"]))
        if feeds and not set(labels) & set(feeds):
            continue
        employer = " / ".join(sorted(set(g["org_name"].dropna()))) or DASH
        title = g["title"].dropna().iloc[0] if g["title"].notna().any() else DASH
        if search and search.casefold() not in f"{employer} {title}".casefold():
            continue
        live = g["live"]
        copies = "unknown" if live.isna().any() else f"{int(truthy(live).sum())} of {len(g)}"
        rows.append({"Employer": employer, "Vacancy": title, "Feeds": " + ".join(labels),
                     "Matched on": g["basis"].iloc[0], "Copies live on Jobiqo": copies})
    return pd.DataFrame(rows, columns=columns).sort_values(["Employer", "Vacancy"], ignore_index=True)


def live_twice(dups: pd.DataFrame) -> int | None:
    """Duplicated vacancies with 2+ copies live on Jobiqo, i.e. advertised more than once."""
    if dups.empty or dups["live"].isna().any():
        return None
    return int((dups.groupby("group_id")["live"].apply(lambda s: truthy(s).sum()) >= 2).sum())


# --------------------------------------------------------------------------- companies

STAGE_LABELS = {"customer": "Customer", "opportunity": "Opportunity", "lead": "Lead",
                "salesqualifiedlead": "Sales qualified lead", "marketingqualifiedlead": "Marketing qualified lead",
                "subscriber": "Subscriber", "evangelist": "Evangelist", "other": "Other"}
NO_STAGE = "Not set"
MATCH_LABELS = {"agree": "✅ Keys agree", "conflict": "⚠️ Keys disagree", "name_only": "🔤 Name only",
                "no_match": "— Not in a feed"}
JOBS_LABELS = {"in_feeds": "In a feed", "jobiqo_only": "Only on Jobiqo", "last_seen": "Not now",
               "never_seen": "None seen"}
# Clean-up reasons, most urgent first; a company can have several.
CLEANUP_ORDER = ("Keys disagree", "Duplicate", "Add Jobiqo ID", "Check Jobiqo ID", "Customer with no jobs")


def stage_label(stage) -> str:
    return STAGE_LABELS.get(stage, stage) if isinstance(stage, str) and stage else NO_STAGE


def stage_options(companies: pd.DataFrame) -> list[str]:
    """The stages present, in HubSpot's order (customer first), custom ones next, "Not set" last."""
    order = list(STAGE_LABELS.values())
    present = set(companies["lifecycle_stage"].map(stage_label)) if not companies.empty else set()
    return sorted(present, key=lambda s: (s == NO_STAGE, order.index(s) if s in order else len(order), s))


def _keys(value) -> list[str]:
    """A BigQuery REPEATED column arrives as a numpy array (or None); a list of strings either way."""
    return [] if value is None or (not hasattr(value, "__len__")) else [str(v) for v in value]


def cleanup_reasons(row) -> list[str]:
    """What needs doing in HubSpot for this company, in CLEANUP_ORDER."""
    reasons = []
    if row["match_status"] == "conflict":
        reasons.append("Keys disagree")
    if isinstance(row["duplicate_on"], str) and row["duplicate_on"]:
        reasons.append("Duplicate")
    if row["match_status"] == "name_only":
        reasons.append("Check Jobiqo ID" if isinstance(row["jobiqo_id"], str) and row["jobiqo_id"] else "Add Jobiqo ID")
    if row["lifecycle_stage"] == "customer" and row["jobs_status"] in ("last_seen", "never_seen"):
        reasons.append("Customer with no jobs")
    return reasons


def _jobs_text(row) -> str:
    if row["jobs_status"] == "last_seen":
        return f"Not now (last in a feed {uk_time(row['last_in_feed_ts']).split(',')[0]})"
    return JOBS_LABELS.get(row["jobs_status"], DASH)


def companies_table(companies: pd.DataFrame, stages: list[str] | None = None, matches: list[str] | None = None,
                    jobs: list[str] | None = None, search: str = "", cleanup_only: bool = False) -> pd.DataFrame:
    """One row per HubSpot company; filters take the display labels. Clean-up items come first
    (keys disagreeing, duplicates, missing or unknown Jobiqo IDs, customers with no jobs), then
    the rest, each by most jobs."""
    columns = ["Company", "Stage", "To do", "Match", "Jobs", "Feed jobs", "On Jobiqo", "Jobiqo ID", "ATS ID",
               "Feed employer", "Same key as", "Notes", "HubSpot ID"]
    if companies.empty:
        return pd.DataFrame(columns=columns)
    rows = companies.copy()
    rows["_stage"] = rows["lifecycle_stage"].map(stage_label)
    rows["_match"] = rows["match_status"].map(MATCH_LABELS).fillna(DASH)
    rows["_jobs"] = rows["jobs_status"].map(JOBS_LABELS).fillna(DASH)
    rows["_reasons"] = rows.apply(cleanup_reasons, axis=1)
    if stages:
        rows = rows[rows["_stage"].isin(stages)]
    if matches:
        rows = rows[rows["_match"].isin(matches)]
    if jobs:
        rows = rows[rows["_jobs"].isin(jobs)]
    if search:
        needle = search.strip().casefold()
        haystack = (rows["name"].fillna("") + " " + rows["feed_name"].fillna("") + " " + rows["hubspot_id"].fillna("")
                    + " " + rows["jobiqo_id"].fillna("") + " " + rows["ats_org_id"].fillna(""))
        rows = rows[haystack.str.casefold().str.contains(needle, regex=False)]
    if cleanup_only:
        rows = rows[rows["_reasons"].map(bool)]
    rank = {reason: i for i, reason in enumerate(CLEANUP_ORDER)}
    rows["_rank"] = rows["_reasons"].map(lambda r: min((rank[x] for x in r), default=len(rank)))
    rows["_feed"] = _int(rows["feed_jobs"])
    rows["_jobiqo"] = _int(rows["jobiqo_jobs"])
    rows = rows.sort_values(["_rank", "_feed", "_jobiqo", "name"], ascending=[True, False, False, True],
                            na_position="last")

    def same_key(r) -> str:
        others = _keys(r["duplicate_ids"])
        return f"{r['duplicate_on']}: {', '.join(others)}" if others else ""

    return pd.DataFrame({
        "Company": rows["name"].fillna("(no name)"),
        "Stage": rows["_stage"],
        "To do": rows["_reasons"].map("; ".join),
        "Match": rows["_match"],
        "Jobs": rows.apply(_jobs_text, axis=1) if len(rows) else pd.Series(dtype="object"),
        "Feed jobs": rows["_feed"],
        "On Jobiqo": rows["_jobiqo"],
        "Jobiqo ID": rows["jobiqo_id"].fillna(""),
        "ATS ID": rows["ats_org_id"].fillna(""),
        "Feed employer": rows["feed_name"].fillna(""),
        "Same key as": rows.apply(same_key, axis=1) if len(rows) else pd.Series(dtype="object"),
        "Notes": rows["notes"].fillna(""),
        "HubSpot ID": rows["hubspot_id"],
    }, columns=columns)


def employers_not_in_hubspot(employers: pd.DataFrame, companies: pd.DataFrame, search: str = "") -> pd.DataFrame:
    """Employers with jobs in a shown feed now that no HubSpot company matched by Jobiqo ID, ATS ID
    or name: one row per employer, its feeds combined, most jobs first."""
    columns = ["Employer", "Jobiqo ID", "Feeds", "Jobs now", "On Jobiqo"]
    matched = {k for keys in companies.get("matched_org_keys", pd.Series(dtype="object")) for k in _keys(keys)}
    rows = employers[truthy(employers["show"]) & _int(employers["jobs_now"]).fillna(0).gt(0)
                     & ~employers["org_key"].isin(matched)]
    if search:
        rows = rows[rows["org_name"].str.contains(search.strip(), case=False, na=False, regex=False)]
    if rows.empty:
        return pd.DataFrame(columns=columns)
    grouped = rows.assign(jobs_now=_int(rows["jobs_now"]), on_jobiqo=_int(rows["on_jobiqo"])).groupby(
        "org_key", sort=False).agg(org_name=("org_name", "first"), feeds=("label", lambda s: ", ".join(sorted(set(s)))),
                                   jobs_now=("jobs_now", "sum"), on_jobiqo=("on_jobiqo", lambda s: s.sum(min_count=1)))
    grouped = grouped.reset_index().sort_values(["jobs_now", "org_name"], ascending=[False, True])
    return pd.DataFrame({
        "Employer": grouped["org_name"],
        "Jobiqo ID": grouped["org_key"].str.partition(":")[2],
        "Feeds": grouped["feeds"],
        "Jobs now": _int(grouped["jobs_now"]),
        "On Jobiqo": _int(grouped["on_jobiqo"]),
    }, columns=columns).reset_index(drop=True)


def companies_totals(companies: pd.DataFrame, employers: pd.DataFrame) -> dict:
    """The pull-out numbers for the Companies tab."""
    if companies.empty:
        return {"companies": 0, "customers": 0, "customers_no_jobs": 0, "cleanup": 0, "employers_not_in_hubspot": 0}
    reasons = companies.apply(cleanup_reasons, axis=1)
    customers = companies["lifecycle_stage"].eq("customer")
    return {
        "companies": len(companies),
        "customers": int(customers.sum()),
        "customers_no_jobs": int(reasons.map(lambda r: "Customer with no jobs" in r).sum()),
        "cleanup": int(reasons.map(lambda r: any(x != "Customer with no jobs" for x in r)).sum()),
        "employers_not_in_hubspot": len(employers_not_in_hubspot(employers, companies)),
    }
