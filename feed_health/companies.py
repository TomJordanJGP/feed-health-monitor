"""Matching HubSpot companies to the employers in the feeds (pure: no I/O).

Each company is looked up three ways against every employer the monitor has seen since it
began (29 Sep 2026):
  * Jobiqo ID ("Jobiqo Org ID") = a feed's organization_id, or Civil Service's jobiqo_id
    (one Jobiqo numbering: org keys "jgp:<n>" and "cs:<n>");
  * ATS ID ("ATS Org ID") = a feed's old_account_id;
  * name, after tidying (normalise_name).
The match status says whether those keys agree, and the duplicate fields list other companies
sharing a key: both feed the HubSpot clean-up. Jobiqo's export numbers employers its own way,
so a company reaches the export through its feed employers (the run's export_orgs links), or by
name when it only posts on Jobiqo directly.
"""
from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from feed_health.feeds import clean_id, display_name
from feed_health.hubspot import Company

# Short forms that differ between the feeds and HubSpot (applied after punctuation is removed).
_ABBREVIATIONS = ((re.compile(r"\bher majesty s\b|\bher majestys\b"), "hm"),
                  (re.compile(r"\bgovt\b"), "government"),
                  (re.compile(r"\bdept\b"), "department"))
_NOISE_WORDS = re.compile(r"\b(the|ltd|limited|plc)\b")

KEY_LABELS = {"jobiqo_id": "Jobiqo ID", "ats_org_id": "ATS ID", "name": "name"}


def normalise_name(raw: str | None) -> str:
    """Case, punctuation and spacing ignored; "&" = "and", "Her Majesty's" = "HM", "Govt" =
    "Government", "Dept" = "Department"; "The", "Ltd", "Limited", "PLC" dropped."""
    name = display_name(raw).lower().replace("&", " and ")
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    for pattern, short in _ABBREVIATIONS:
        name = pattern.sub(short, name)
    return re.sub(r"\s+", " ", _NOISE_WORDS.sub(" ", name)).strip()


@dataclass(frozen=True)
class Employer:
    org_key: str                  # "jgp:934" or "cs:4204": the number is the Jobiqo ID either way
    name: str                     # its latest name in a feed (Civil Service: Jobiqo's name for it)
    names: frozenset[str]         # every name it has had, for name matching
    ats_ids: frozenset[str]

    @property
    def jobiqo_id(self) -> str:
        return self.org_key.partition(":")[2]


def merge_employers(history: dict[str, Employer], current_names: Iterable[tuple[str, str]],
                    current_ats: Iterable[tuple[str, str]]) -> dict[str, Employer]:
    """The stored history plus this run's (not yet written) names and ATS IDs; this run's name wins."""
    names: dict[str, str] = {k: e.name for k, e in history.items()}
    aliases: dict[str, set[str]] = defaultdict(set, {k: set(e.names) for k, e in history.items()})
    ats: dict[str, set[str]] = defaultdict(set, {k: set(e.ats_ids) for k, e in history.items()})
    for org_key, name in current_names:
        if org_key != "none:" and name:
            names[org_key] = name
            aliases[org_key].add(name)
    for org_key, ats_id in current_ats:
        if org_key != "none:" and ats_id:
            ats[org_key].add(ats_id)
    keys = names.keys() | ats.keys()
    return {k: Employer(k, names.get(k, ""), frozenset(aliases.get(k, ())), frozenset(ats.get(k, ())))
            for k in keys}


def match_companies(companies: list[Company], employers: dict[str, Employer], export_orgs: list[dict],
                    synced_ts: datetime) -> list[dict]:
    """One `companies` row per HubSpot company. match_status, from the first that applies:
      conflict   its keys found different employers (the IDs are believed; notes say what disagreed)
      agree      its Jobiqo or ATS ID found an employer, and every other key that found one found it too
      name_only  only its name found an employer (IDs empty, or not in any feed)
      no_match   nothing found
    """
    by_jobiqo_id: dict[str, set[str]] = defaultdict(set)
    by_ats: dict[str, set[str]] = defaultdict(set)
    by_name: dict[str, set[str]] = defaultdict(set)
    for e in employers.values():
        by_jobiqo_id[e.jobiqo_id].add(e.org_key)
        for ats_id in e.ats_ids:
            by_ats[ats_id].add(e.org_key)
        for name in e.names:
            if key := normalise_name(name):
                by_name[key].add(e.org_key)

    export_by_employer: dict[str, set[str]] = defaultdict(set)
    export_by_name: dict[str, set[str]] = defaultdict(set)
    for x in export_orgs:
        if x.get("org_key"):
            export_by_employer[x["org_key"]].add(x["export_org_id"])
        if key := normalise_name(x.get("company")):
            export_by_name[key].add(x["export_org_id"])

    keyed = [(c, {"jobiqo_id": clean_id(c.jobiqo_id), "ats_org_id": clean_id(c.ats_org_id),
                  "name": normalise_name(c.name)}) for c in companies]
    sharing: dict[tuple[str, str], list[str]] = defaultdict(list)
    for c, keys in keyed:
        for field_name, value in keys.items():
            if value:
                sharing[(field_name, value)].append(c.hubspot_id)

    index = {"jobiqo_id": by_jobiqo_id, "ats_org_id": by_ats, "name": by_name}
    rows = []
    for c, keys in keyed:
        found = {f: index[f].get(v, set()) for f, v in keys.items() if v and index[f].get(v)}
        # Only claim an ID is in no feed when the monitor holds some of that kind (ATS IDs are
        # stored from 29 Oct 2026, as each feed next changes).
        notes = [f"{KEY_LABELS[f]} {v} not in any feed" for f, v in keys.items()
                 if v and f != "name" and f not in found and index[f]]

        id_hits = found.get("jobiqo_id") or found.get("ats_org_id")
        if not found:
            status, matched = "no_match", set()
        elif not id_hits:
            status, matched = "name_only", found["name"]
        else:
            common = set.intersection(*found.values())
            status, matched = ("agree", common) if common else ("conflict", id_hits)
        for f, hits in found.items():
            if not hits & matched:
                notes.append(f"{KEY_LABELS[f]} points to {_names(hits, employers)}")
        if len(matched) > 1:
            notes.append(f"matches {len(matched)} feed employers")
        feed_name = _names(matched, employers) if matched else None
        if matched and "name" not in found and keys["name"]:
            notes.append(f"feed name differs: {feed_name}")

        export_ids = set().union(*(export_by_employer.get(k, set()) for k in matched)) \
            or export_by_name.get(keys["name"], set())
        shared_on = [f for f, v in keys.items() if v and len(sharing[(f, v)]) > 1]
        others = sorted({h for f in shared_on for h in sharing[(f, keys[f])]} - {c.hubspot_id}, key=_id_order)
        rows.append({
            "synced_ts": synced_ts, "hubspot_id": c.hubspot_id, "name": c.name or None,
            "jobiqo_id": keys["jobiqo_id"] or None, "ats_org_id": keys["ats_org_id"] or None,
            "lifecycle_stage": c.lifecycle_stage or None,
            "match_status": status, "matched_by": ", ".join(f for f in found if found[f] & matched) or None,
            "matched_org_keys": sorted(matched), "feed_name": feed_name,
            "export_org_ids": sorted(export_ids, key=_id_order),
            "duplicate_on": ", ".join(KEY_LABELS[f] for f in shared_on) or None,
            "duplicate_ids": others, "notes": "; ".join(notes) or None,
        })
    return rows


def summary(rows: list[dict]) -> dict[str, int]:
    """Counts for the run log (which is public: counts only, never names)."""
    counts = Counter(r["match_status"] for r in rows)
    return {"companies": len(rows), **{s: counts.get(s, 0) for s in ("agree", "conflict", "name_only", "no_match")},
            "duplicates": sum(1 for r in rows if r["duplicate_on"]),
            "on_jobiqo_export": sum(1 for r in rows if r["export_org_ids"])}


def _names(org_keys: set[str], employers: dict[str, Employer]) -> str:
    return " / ".join(sorted({employers[k].name or k for k in org_keys}))


def _id_order(value: str) -> tuple[int, str]:
    return (len(value), value)
