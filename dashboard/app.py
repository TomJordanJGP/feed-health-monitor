"""Feed health dashboard.

  venv/bin/streamlit run dashboard/app.py --server.port 8503

Reads the feed_health views (production by default; FEED_HEALTH_DASHBOARD_DATASET=feed_health_dev
for the local dataset). Credentials: st.secrets["gcp_service_account"] when hosted, otherwise
the collector key named in .env.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # project root, for feed_health/dashboard

from dashboard import helpers  # noqa: E402
from feed_health.config import load_settings  # noqa: E402

LOGO = "https://media.jobsgopublic.com/wp-content/uploads/2025/10/JGP-Logo_RGB.png"
SCOPES = ["https://www.googleapis.com/auth/bigquery"]

st.set_page_config(page_title="Feed health · Jobs Go Public", page_icon="🩺", layout="wide")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
html, body, [class*="css"], .stMarkdown, .stDataFrame, button, input { font-family: 'DM Sans', sans-serif; }
h1, h2, h3 { font-family: 'DM Sans', sans-serif; font-weight: 700; color: #643791; }
</style>
""", unsafe_allow_html=True)
st.logo(LOGO, size="large")

settings = load_settings(require_urls=False)
DATASET = os.environ.get("FEED_HEALTH_DASHBOARD_DATASET", "feed_health")


@st.cache_resource
def client() -> bigquery.Client:
    try:
        info = st.secrets["gcp_service_account"]
        creds = Credentials.from_service_account_info(dict(info), scopes=SCOPES)
    except (KeyError, FileNotFoundError, st.errors.StreamlitSecretNotFoundError):
        creds = Credentials.from_service_account_file(str(settings.credentials_path), scopes=SCOPES)
    return bigquery.Client(project=settings.bq_project, credentials=creds, location=settings.bq_location)


def show_table(table: pd.DataFrame, **kwargs) -> None:
    """Numbers stay numeric (sortable); missing values display as a dash rather than 'None'."""
    st.dataframe(table.style.format(na_rep=helpers.DASH), hide_index=True, width="stretch", **kwargs)


@st.cache_data(ttl=300, show_spinner=False)
def view(name: str) -> pd.DataFrame:
    sql = f"SELECT * FROM `{settings.bq_project}.{DATASET}.{name}`"
    return client().query_and_wait(sql).to_dataframe(create_bqstorage_client=False)


# --------------------------------------------------------------------------- header

st.title("Feed health")
st.caption("Is each feed being rebuilt, and are any employers' jobs disappearing or not reaching Jobiqo?")

with st.spinner("Loading the latest checks…"):
    status, summary, employers = view("v_feed_status"), view("v_summary"), view("v_employers")
    duplicates = view("v_duplicates")

if status.empty:
    st.info(f"No checks recorded in `{DATASET}` yet. The collector runs hourly at :47.")
    st.stop()

last_run = status["check_ts"].max()
now = datetime.now(timezone.utc)
mon = settings.monitor
monitor = helpers.monitor_state(last_run, now, mon.monitor_orange_hours, mon.monitor_red_hours)
minutes = int((pd.Timestamp(now) - pd.Timestamp(last_run)).total_seconds() // 60)
head_left, head_right = st.columns([4, 1])
head_left.markdown(f"**Last check:** {helpers.uk_time(last_run)} ({helpers.age_label(minutes / 60)} ago) "
                   f"{helpers.STATUS_LABELS[monitor].split()[0]}")
if head_right.button("Refresh", width="stretch"):
    st.cache_data.clear()
    st.rerun()
if monitor != "green":
    (st.error if monitor == "red" else st.warning)(
        f"The monitor hasn't recorded a check for {helpers.age_label(minutes / 60)}. The figures below may be "
        "out of date: check the collector's GitHub Action (TomJordanJGP/feed-health-monitor).")

tab_status, tab_summary, tab_employers, tab_dups = st.tabs(["Feed status", "Summary", "Employers", "Duplicates"])


def number(value) -> str:
    return "unavailable" if value is None or pd.isna(value) else f"{int(value):,}"


# --------------------------------------------------------------------------- feed status

with tab_status:
    incoming = helpers.incoming_table(status)
    counts = incoming["Status"].value_counts()
    st.subheader("Incoming feeds")
    st.markdown(" · ".join(f"{label} **{counts.get(label, 0)}**" for label in
                           (helpers.STATUS_LABELS[s] for s in ("green", "orange", "red", "error"))
                           if counts.get(label, 0)))
    show_table(incoming)
    st.caption(f"The feeds Jobiqo imports. Age is the time between when the feed's XML was last rebuilt and "
               f"the check: orange after {settings.default_orange_hours:g} h, red after "
               f"{settings.default_red_hours:g} h (set per feed in config/feeds.yml). Jobiqo imports every ~3 h, "
               f"but can only pick up changes once the XML has been rebuilt.")
    st.subheader("Outgoing feed")
    show_table(helpers.outgoing_table(status))
    st.caption("What Jobiqo publishes: every live job on the board, including jobs created directly on the "
               "site. Employers are distinct Jobiqo organisations.")

# --------------------------------------------------------------------------- summary

with tab_summary:
    totals = helpers.summary_totals(summary)
    twice = helpers.live_twice(duplicates)
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Employers", number(totals["employers"]))
    c2.metric("Vacancies in feeds", number(totals["vacancies"]))
    live = totals["live"]
    share = None if pd.isna(live) or not totals["vacancies"] else f"{live / totals['vacancies']:.0%} of listings"
    c3.metric("Live on Jobiqo", number(live), share, delta_color="off", delta_arrow="off")
    c4.metric("Duplicate vacancies", number(totals["duplicates"]),
              None if twice is None else f"{twice:,} live twice", delta_color="off", delta_arrow="off")
    c5.metric("Not reaching Jobiqo", number(totals["not_reaching"]))
    show_table(helpers.summary_table(summary))
    st.caption("Totals are plain sums of each column, so a vacancy or employer carried by two feeds counts in "
               "both (see Duplicates). Added and removed compare each feed's latest version with its previous "
               "one (\"Changes since\"). **Live on Jobiqo:** listings from the feed that are published on "
               f"Jobiqo. **Not reaching Jobiqo:** listings in a feed for {mon.not_reaching_grace_hours:g} h+ "
               "that aren't past their closing date and aren't live in any form.")

# --------------------------------------------------------------------------- employers

with tab_employers:
    feed_labels = sorted(employers.loc[helpers.truthy(employers["show"]), "label"].dropna().unique())
    f1, f2, f3 = st.columns([3, 3, 1.4])
    chosen = f1.multiselect("Feeds", feed_labels, placeholder="All feeds")
    search = f2.text_input("Employer", placeholder="Search by name")
    issues_only = f3.toggle("Issues only", value=True)
    table = helpers.employers_table(employers, chosen, search, issues_only)
    if table.empty and issues_only:
        st.success("No employer issues in the latest checks. Switch off **Issues only** to see every employer.")
    else:
        show_table(table, height=min(38 * (len(table) + 1), 700))
    st.caption("**On Jobiqo:** that employer's listings in that feed which are published on Jobiqo, next to how "
               "many the feed lists (Jobs this check). **None** means there is nothing to compare yet: the "
               "feed has only one version so far. **Issues:** an employer no longer showing, jobs removed "
               "before their closing date, or jobs not reaching Jobiqo. **Expired (expected):** gone within a "
               "day of their closing date. **Removed (no closing date):** can't be judged either way; Backfill "
               "V1 carries no closing dates. **Moved:** the same vacancy is still live in another imported feed.")

# --------------------------------------------------------------------------- duplicates

with tab_dups:
    dup_labels = sorted(duplicates["label"].dropna().unique()) if not duplicates.empty else []
    d1, d2 = st.columns([3, 3])
    dup_feeds = d1.multiselect("Feeds", dup_labels, placeholder="All feeds", key="dup_feeds")
    dup_search = d2.text_input("Employer or vacancy", placeholder="Search", key="dup_search")
    table = helpers.duplicates_table(duplicates, dup_feeds, dup_search)
    groups = duplicates["group_id"].nunique() if not duplicates.empty else 0
    if groups == 0:
        st.success("No vacancy is listed in more than one feed.")
    else:
        st.markdown(f"**{groups:,}** vacancies are listed in two feeds"
                    + (f"; **{twice:,}** of them are live on Jobiqo twice." if twice is not None else "."))
        show_table(table, height=min(38 * (len(table) + 1), 700))
    st.caption("The same vacancy under different IDs in two feeds, matched on the same advert URL or the same "
               "employer and reference. Jobiqo imports each copy, so every live copy is a separate advert.")

st.caption(f"Dataset `{settings.bq_project}.{DATASET}` · statuses as of {helpers.uk_time(last_run)} UK time")
