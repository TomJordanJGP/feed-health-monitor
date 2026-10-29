"""Matching HubSpot companies to feed employers (synthetic names and IDs only)."""
from datetime import datetime, timezone

import pytest

from feed_health.companies import Employer, match_companies, merge_employers, normalise_name, summary
from feed_health.hubspot import Company

SYNCED = datetime(2026, 10, 30, 6, 47, tzinfo=timezone.utc)


@pytest.mark.parametrize("raw, normalised", [
    ("Sefton Council", "sefton council"),
    ("  Bath &  North East Somerset ", "bath and north east somerset"),
    ("Her Majesty's Prison & Probation Service", "hm prison and probation service"),
    ("Dept for Work & Pensions", "department for work and pensions"),
    ("Govt Legal Department", "government legal department"),
    ("The Riverside Group Ltd.", "riverside group"),
    ("", ""), (None, ""),
])
def test_normalise_name(raw, normalised):
    assert normalise_name(raw) == normalised


def employer(org_key, name, ats=(), aliases=()):
    return Employer(org_key, name, frozenset({name, *aliases}), frozenset(ats))


EMPLOYERS = {e.org_key: e for e in (
    employer("jgp:934", "Sefton Council", ats={"5512"}),
    employer("jgp:300", "Medway Council", ats={"901"}),
    employer("jgp:77", "Kent County Council"),
    employer("jgp:100", "Surrey County Council"),
    employer("jgp:5201", "HM Prison and Probation Service"),   # one Jobiqo numbering:
    employer("cs:5201", "HM Prison and Probation Service"),    # the same employer in two feeds
)}
EXPORT_ORGS = [
    {"export_org_id": "1047", "company": "Sefton Council", "org_key": "jgp:934"},
    {"export_org_id": "2001", "company": "Direct Posting Trust", "org_key": None},
]


def company(hubspot_id, name, jobiqo_id="", ats_org_id="", stage="customer"):
    return Company(hubspot_id, name, jobiqo_id, ats_org_id, stage)


@pytest.fixture
def rows():
    companies = [
        company("1", "Sefton Council", jobiqo_id="934", ats_org_id="5512"),
        company("2", "Kent County Council", ats_org_id="901"),
        company("3", "Surrey County Council Ltd"),
        company("4", "Direct Posting Trust", stage=""),
        company("5", "Her Majesty's Prison & Probation Service", jobiqo_id="5201"),
        company("6", "Sefton Council"),
        company("7", "Kent CC", jobiqo_id="77"),
        company("8", "Brand New Council", jobiqo_id="9999"),
    ]
    return {r["hubspot_id"]: r for r in match_companies(companies, EMPLOYERS, EXPORT_ORGS, SYNCED)}


def test_every_key_agreeing(rows):
    r = rows["1"]
    assert (r["match_status"], r["matched_by"], r["matched_org_keys"]) == ("agree", "jobiqo_id, ats_org_id, name", ["jgp:934"])
    assert (r["feed_name"], r["export_org_ids"], r["synced_ts"], r["lifecycle_stage"]) == \
        ("Sefton Council", ["1047"], SYNCED, "customer")


def test_keys_finding_different_employers_is_a_conflict_and_the_id_wins(rows):
    r = rows["2"]
    assert (r["match_status"], r["matched_org_keys"], r["matched_by"]) == ("conflict", ["jgp:300"], "ats_org_id")
    assert r["notes"] == "name points to Kent County Council"


def test_name_only_after_tidying(rows):
    r = rows["3"]
    assert (r["match_status"], r["matched_org_keys"], r["jobiqo_id"], r["ats_org_id"]) == \
        ("name_only", ["jgp:100"], None, None)


def test_no_feed_employer_but_on_jobiqo_by_name(rows):
    r = rows["4"]
    assert (r["match_status"], r["matched_org_keys"], r["export_org_ids"], r["lifecycle_stage"]) == \
        ("no_match", [], ["2001"], None)


def test_one_jobiqo_id_in_two_feeds(rows):
    r = rows["5"]
    assert (r["match_status"], r["matched_org_keys"]) == ("agree", ["cs:5201", "jgp:5201"])
    assert r["notes"] == "matches 2 feed employers"


def test_companies_sharing_a_key_list_each_other(rows):
    assert (rows["1"]["duplicate_on"], rows["1"]["duplicate_ids"]) == ("name", ["6"])
    assert (rows["6"]["duplicate_on"], rows["6"]["duplicate_ids"]) == ("name", ["1"])
    assert (rows["6"]["match_status"], rows["6"]["matched_org_keys"]) == ("name_only", ["jgp:934"])
    assert rows["3"]["duplicate_on"] is None and rows["3"]["duplicate_ids"] == []


def test_id_match_with_a_different_name_is_noted(rows):
    r = rows["7"]
    assert (r["match_status"], r["matched_by"]) == ("agree", "jobiqo_id")
    assert r["notes"] == "feed name differs: Kent County Council"


def test_an_id_no_feed_has_seen_is_noted(rows):
    r = rows["8"]
    assert (r["match_status"], r["notes"]) == ("no_match", "Jobiqo ID 9999 not in any feed")


def test_no_claim_about_ats_ids_before_any_are_stored():
    no_ats = {k: Employer(e.org_key, e.name, e.names, frozenset()) for k, e in EMPLOYERS.items()}
    (row,) = match_companies([company("9", "Nowhere Ltd", jobiqo_id="9999", ats_org_id="42")], no_ats, [], SYNCED)
    assert row["notes"] == "Jobiqo ID 9999 not in any feed"


def test_summary_counts_only(rows):
    assert summary(list(rows.values())) == {"companies": 8, "agree": 3, "conflict": 1, "name_only": 2, "no_match": 2,
                                            "duplicates": 2, "on_jobiqo_export": 3}


def test_merge_employers_adds_this_runs_names_and_ats_ids():
    history = {"jgp:934": employer("jgp:934", "Sefton MBC", ats={"5512"})}
    merged = merge_employers(history, [("jgp:934", "Sefton Council"), ("jgp:1", "New Employer"), ("none:", "x")],
                             [("jgp:934", "6000"), ("jgp:1", ""), ("none:", "7")])
    assert set(merged) == {"jgp:934", "jgp:1"}
    sefton = merged["jgp:934"]
    assert (sefton.name, sefton.names, sefton.ats_ids) == \
        ("Sefton Council", {"Sefton MBC", "Sefton Council"}, {"5512", "6000"})
    assert merged["jgp:1"].ats_ids == frozenset() and merged["jgp:1"].jobiqo_id == "1"
