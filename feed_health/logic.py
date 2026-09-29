"""Pure logic, no I/O: version diffs, gone-job classification, cross-feed clusters,
"not reaching Jobiqo", and summaries.

Vocabulary
  version  one content state of a feed. A rebuild with different content is a new version.
  node     one job in one version: (source, version_id, job_id).
  cluster  nodes that are the same vacancy: the same job across versions of a feed, or across
           feeds by id, normalised URL or org+reference. A key links nodes only if it is
           unique within its own version, because ATS has hundreds of empty URLs and shared
           references ("External") that would otherwise chain unrelated jobs together.
"""
from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta

from feed_health.feeds import Job, JobiqoExport, name_key, norm_ref, ref_is_usable

Node = tuple[str, str, str]  # (source, version_id, job_id)

EXPIRED = "expired"
MOVED = "moved"
REMOVED_UNEXPECTED = "removed_unexpected"
REMOVED_NO_CLOSE = "removed_no_close"


@dataclass(frozen=True)
class Version:
    source: str
    version_id: str
    build_ts: datetime
    jobs: tuple[Job, ...]
    first_seen: dict[str, datetime] = field(default_factory=dict, compare=False)  # job_id -> first build

    def first_seen_of(self, job_id: str) -> datetime:
        return self.first_seen.get(job_id, self.build_ts)


# --------------------------------------------------------------------------- clusters

def _keys(job: Job) -> list[tuple]:
    keys: list[tuple] = [("id", job.job_id)]
    if job.url_norm:
        keys.append(("url", job.url_norm))
    if job.org_key != "none:" and ref_is_usable(job.reference):
        keys.append(("ref", job.org_key, norm_ref(job.reference)))
    return keys


class Clusters:
    """Union-find over the nodes of the given versions."""

    def __init__(self, versions: Iterable[Version]):
        self._parent: dict[Node, Node] = {}
        self._job: dict[Node, Job] = {}
        owner: dict[tuple, Node] = {}
        for v in versions:
            counts = Counter(k for job in v.jobs for k in _keys(job))
            for job in v.jobs:
                node = (v.source, v.version_id, job.job_id)
                self._parent.setdefault(node, node)
                self._job[node] = job
                for key in _keys(job):
                    if counts[key] != 1:
                        continue  # ambiguous within this version: never link on it
                    if key in owner:
                        self._union(node, owner[key])
                    else:
                        owner[key] = node
        self._members: dict[Node, list[Node]] = defaultdict(list)
        for node in self._parent:
            self._members[self.root(node)].append(node)

    def root(self, node: Node) -> Node:
        parent = self._parent
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    def _union(self, a: Node, b: Node) -> None:
        ra, rb = self.root(a), self.root(b)
        if ra != rb:
            self._parent[rb] = ra

    def members(self, node: Node) -> list[Node]:
        return self._members[self.root(node)]

    def job(self, node: Node) -> Job:
        return self._job[node]

    def same_version_conflicts(self) -> int:
        """Clusters holding 2+ jobs from one version: two adverts one key says are the same.
        Reported as a data-quality signal rather than trusted."""
        return sum(1 for nodes in self._members.values()
                   if len({(s, v) for s, v, _ in nodes}) < len(nodes))


# --------------------------------------------------------------------------- version diff

def match_versions(prev: Version, curr: Version) -> dict[str, tuple[str, bool]]:
    """prev job_id -> (curr job_id, rehashed). Matches by job_id, then by (org, reference)
    where the reference is usable and the pair is unique among the unmatched jobs on both
    sides. The latter is a URL change, which alters the id hash but isn't the job going."""
    curr_ids = {j.job_id for j in curr.jobs}
    prev_ids = {j.job_id for j in prev.jobs}
    matched = {j.job_id: (j.job_id, False) for j in prev.jobs if j.job_id in curr_ids}

    def unmatched_by_ref(jobs: tuple[Job, ...], other_ids: set[str]) -> dict[tuple, list[str]]:
        index: dict[tuple, list[str]] = defaultdict(list)
        for j in jobs:
            if j.job_id not in other_ids and j.org_key != "none:" and ref_is_usable(j.reference):
                index[(j.org_key, norm_ref(j.reference))].append(j.job_id)
        return index

    gone_by_ref = unmatched_by_ref(prev.jobs, curr_ids)
    new_by_ref = unmatched_by_ref(curr.jobs, prev_ids)
    for key in gone_by_ref.keys() & new_by_ref.keys():
        if len(gone_by_ref[key]) == 1 and len(new_by_ref[key]) == 1:
            matched[gone_by_ref[key][0]] = (new_by_ref[key][0], True)
    return matched


def carry_first_seen(prev: Version | None, curr_jobs: tuple[Job, ...], curr_build: datetime,
                     matched: dict[str, tuple[str, bool]]) -> dict[str, datetime]:
    """When each job first appeared: carried forward for continuing jobs (including rehashes),
    else the build time of the version that introduced it."""
    first = {j.job_id: curr_build for j in curr_jobs}
    if prev is not None:
        for prev_id, (curr_id, _) in matched.items():
            first[curr_id] = prev.first_seen_of(prev_id)
    return first


@dataclass(frozen=True)
class DiffContext:
    clusters: Clusters
    latest_version_ids: dict[str, str]   # source -> its latest version after this run
    imported_sources: frozenset[str]     # feeds Jobiqo imports: the only valid "moved" targets
    expiry_grace: timedelta


def classify_gone(job: Job, source: str, prev_version_id: str, curr_build: datetime,
                  ctx: DiffContext) -> str:
    """Why a job left a feed, in order of precedence:
      expired             its close date (its own; a twin's only if it has none) is before the
                          new build + grace. Feeds remove jobs the morning they close.
      moved               the same vacancy is in another *imported* feed's latest version
      removed_unexpected  it has a close date, still in the future
      removed_no_close    no close date anywhere (all of Backfill V1, some scraped employers)
    """
    twins = [n for n in ctx.clusters.members((source, prev_version_id, job.job_id)) if n[0] != source]
    close = job.close_date
    if close is None:
        twin_dates = [ctx.clusters.job(n).close_date for n in twins if ctx.clusters.job(n).close_date]
        close = max(twin_dates) if twin_dates else None
    if close is not None and close < curr_build + ctx.expiry_grace:
        return EXPIRED
    if any(s in ctx.imported_sources and ctx.latest_version_ids.get(s) == v for s, v, _ in twins):
        return MOVED
    return REMOVED_UNEXPECTED if close is not None else REMOVED_NO_CLOSE


@dataclass(frozen=True)
class EmployerCheck:
    source: str
    version_id: str
    prev_version_id: str | None
    build_ts: datetime
    prev_build_ts: datetime | None
    org_key: str
    org_name: str
    still_showing: bool
    jobs_now: int
    jobs_prev: int | None          # None on a feed's first version ("first check")
    added: int | None
    expired: int | None
    moved: int | None
    removed_unexpected: int | None
    removed_no_close: int | None
    rehashed: int | None


def employer_checks(prev: Version | None, curr: Version, matched: dict[str, tuple[str, bool]],
                    ctx: DiffContext, names: dict[str, str]) -> list[EmployerCheck]:
    now_count = Counter(j.org_key for j in curr.jobs)
    if prev is None:
        return [EmployerCheck(curr.source, curr.version_id, None, curr.build_ts, None, org,
                              name_for(org, names), True, n, None, None, None, None, None, None, None)
                for org, n in sorted(now_count.items())]

    prev_count = Counter(j.org_key for j in prev.jobs)
    matched_curr = {c for c, _ in matched.values()}
    added = Counter(j.org_key for j in curr.jobs if j.job_id not in matched_curr)
    rehashed = Counter(j.org_key for j in prev.jobs if matched.get(j.job_id, ("", False))[1])
    gone: dict[str, Counter] = defaultdict(Counter)
    for j in prev.jobs:
        if j.job_id not in matched:
            gone[j.org_key][classify_gone(j, prev.source, prev.version_id, curr.build_ts, ctx)] += 1

    rows = []
    for org in sorted(prev_count.keys() | now_count.keys()):
        g = gone[org]
        rows.append(EmployerCheck(
            curr.source, curr.version_id, prev.version_id, curr.build_ts, prev.build_ts, org,
            name_for(org, names), now_count[org] > 0, now_count[org], prev_count[org], added[org],
            g[EXPIRED], g[MOVED], g[REMOVED_UNEXPECTED], g[REMOVED_NO_CLOSE], rehashed[org]))
    return rows


# --------------------------------------------------------------------------- Jobiqo

@dataclass(frozen=True)
class Gap:
    source: str
    org_key: str
    org_name: str
    job_id: str
    title: str
    first_seen: datetime
    close_date: datetime | None
    node: Node


def not_reaching(versions: Iterable[Version], clusters: Clusters, export_ids: frozenset[str],
                 run_ts: datetime, grace: timedelta, names: dict[str, str]) -> list[Gap]:
    """Jobs in a feed long enough to have been imported (first seen >= grace ago), not yet past
    their close date (Jobiqo won't show those anyway), and not live on Jobiqo, neither as
    themselves nor as any cross-feed twin."""
    gaps = []
    for v in versions:
        for job in v.jobs:
            if job.close_date is not None and job.close_date < run_ts:
                continue
            first = v.first_seen_of(job.job_id)
            if first > run_ts - grace or job.job_id in export_ids:
                continue
            node = (v.source, v.version_id, job.job_id)
            if any(n[2] in export_ids for n in clusters.members(node)):
                continue
            gaps.append(Gap(v.source, job.org_key, name_for(job.org_key, names), job.job_id,
                            job.title, first, job.close_date, node))
    return gaps


def export_problem(jobs_now: int, jobs_prev: int | None, last_modified: datetime | None,
                   run_ts: datetime, max_shrink: float, max_age: timedelta) -> str | None:
    """Why this run's Jobiqo export can't be trusted for "not reaching" (None = usable).
    A partial or stale export would otherwise flag thousands of healthy jobs."""
    if jobs_now == 0:
        return "export has no jobs"
    if jobs_prev and jobs_now < (1 - max_shrink) * jobs_prev:
        return f"export shrank from {jobs_prev} to {jobs_now} jobs"
    if last_modified is not None and run_ts - last_modified > max_age:
        return f"export not updated for {(run_ts - last_modified).total_seconds() / 3600:.0f}h"
    return None


# --------------------------------------------------------------------------- names

def employer_names(versions: Iterable[Version], export: JobiqoExport | None) -> dict[str, str]:
    """Display name per org_key. JGP feeds carry organization_name. Civil Service carries only
    jobiqo_id, so its name is the Jobiqo company most often attached to its jobs."""
    names: dict[str, str] = {}
    cs_votes: dict[str, Counter] = defaultdict(Counter)
    stored: dict[str, str] = {}
    for v in versions:
        for j in v.jobs:
            if j.org_key.startswith("cs:"):
                if export is not None and j.job_id in export.company_by_id and export.company_by_id[j.job_id]:
                    cs_votes[j.org_key][export.company_by_id[j.job_id]] += 1
                elif j.org_name:
                    stored.setdefault(j.org_key, j.org_name)
            elif j.org_name:
                names.setdefault(j.org_key, j.org_name)
    for org, votes in cs_votes.items():
        names[org] = votes.most_common(1)[0][0]
    for org, name in stored.items():
        names.setdefault(org, name)
    return names


def name_for(org_key: str, names: dict[str, str]) -> str:
    if org_key in names:
        return names[org_key]
    if org_key.startswith("cs:"):
        return f"CS dept {org_key[3:]}"
    return "(no employer)" if org_key == "none:" else org_key


def with_names(version: Version, names: dict[str, str]) -> Version:
    """Fill empty org_names (Civil Service) before a version is stored."""
    jobs = tuple(j if j.org_name else replace(j, org_name=name_for(j.org_key, names)) for j in version.jobs)
    return replace(version, jobs=jobs)


# --------------------------------------------------------------------------- summaries

@dataclass(frozen=True)
class Summary:
    employers_total: int
    employers_added: int | None      # None when there is no previous version to compare
    employers_removed: int | None
    vacancies_total: int
    vacancies_added: int | None
    vacancies_removed: int | None


def feed_summary(prev: Version | None, curr: Version, matched: dict[str, tuple[str, bool]] | None) -> Summary:
    orgs_now = {j.org_key for j in curr.jobs}
    if prev is None or matched is None:
        return Summary(len(orgs_now), None, None, len(curr.jobs), None, None)
    orgs_prev = {j.org_key for j in prev.jobs}
    matched_curr = {c for c, _ in matched.values()}
    return Summary(len(orgs_now), len(orgs_now - orgs_prev), len(orgs_prev - orgs_now),
                   len(curr.jobs), len(curr.jobs) - len(matched_curr), len(prev.jobs) - len(matched))


def all_feeds_summary(latest: list[Version], previous: list[Version], clusters: Clusters,
                      names: dict[str, str]) -> Summary:
    """Across feeds, each vacancy (cluster) and employer (normalised name, which also merges the
    JGP and Civil Service ID namespaces) counts once. "Previous" is each feed's previous
    version (its latest if it has none yet), so a vacancy or employer moving between feeds
    nets to zero, and a feed's first check adds nothing."""
    def vacancies(versions: list[Version]) -> set[Node]:
        return {clusters.root((v.source, v.version_id, j.job_id)) for v in versions for j in v.jobs}

    def employers(versions: list[Version]) -> set[str]:
        return {name_key(name_for(j.org_key, names)) for v in versions for j in v.jobs}

    vac_now, vac_prev = vacancies(latest), vacancies(previous)
    emp_now, emp_prev = employers(latest), employers(previous)
    return Summary(len(emp_now), len(emp_now - emp_prev), len(emp_prev - emp_now),
                   len(vac_now), len(vac_now - vac_prev), len(vac_prev - vac_now))


def distinct_gaps(gaps: Iterable[Gap], clusters: Clusters) -> int:
    """Vacancies not reaching Jobiqo, each counted once however many feeds carry it."""
    return len({clusters.root(g.node) for g in gaps})


# --------------------------------------------------------------------------- live and duplicates

@dataclass(frozen=True)
class LiveCounts:
    by_source: dict[str, int]              # source -> its listings published on Jobiqo
    by_employer: dict[tuple[str, str], int]  # (source, org_key) -> live listings (0 when none)


def live_listings(versions: Iterable[Version], export_ids: frozenset[str]) -> LiveCounts:
    """Which listings are published on Jobiqo, by their own id. Unlike "not reaching", a twin in
    another feed doesn't count: this answers "how many of this feed's (or employer's) jobs are
    actually on the board", which sits next to how many the feed lists."""
    by_source: dict[str, int] = {}
    by_employer: dict[tuple[str, str], int] = {}
    for v in versions:
        by_source.setdefault(v.source, 0)
        for j in v.jobs:
            key = (v.source, j.org_key)
            live = j.job_id in export_ids
            by_employer[key] = by_employer.get(key, 0) + live
            by_source[v.source] += live
    return LiveCounts(by_source, by_employer)


@dataclass(frozen=True)
class DuplicateMember:
    source: str
    job_id: str
    org_key: str
    org_name: str
    title: str
    live: bool | None        # None when the export can't be trusted this run


@dataclass(frozen=True)
class DuplicateGroup:
    group_id: str
    basis: str               # how the copies were matched: same ID / same URL / same reference / linked
    members: tuple[DuplicateMember, ...]


def _basis(jobs: list[Job]) -> str:
    if len({j.job_id for j in jobs}) == 1:
        return "same ID"
    if jobs[0].url_norm and len({j.url_norm for j in jobs}) == 1:
        return "same URL"
    if len({(j.org_key, norm_ref(j.reference)) for j in jobs}) == 1:
        return "same reference"
    return "linked"          # a chain: A~B by URL, B~C by reference


def duplicate_groups(versions: list[Version], clusters: Clusters, export_ids: frozenset[str] | None,
                     names: dict[str, str]) -> list[DuplicateGroup]:
    """The same vacancy listed in two or more of the given feeds (normally each feed's latest
    version). Each copy imported into Jobiqo is a separate advert on the board."""
    by_root: dict[Node, list[tuple[str, Job]]] = defaultdict(list)
    for v in versions:
        for j in v.jobs:
            by_root[clusters.root((v.source, v.version_id, j.job_id))].append((v.source, j))
    groups = []
    for members in by_root.values():
        if len({source for source, _ in members}) < 2:
            continue
        members = sorted(members, key=lambda m: (m[0], m[1].job_id))
        group_id = hashlib.md5("|".join(f"{s}:{j.job_id}" for s, j in members).encode()).hexdigest()[:12]
        groups.append(DuplicateGroup(group_id, _basis([j for _, j in members]), tuple(
            DuplicateMember(s, j.job_id, j.org_key, name_for(j.org_key, names), j.title,
                            None if export_ids is None else j.job_id in export_ids)
            for s, j in members)))
    return sorted(groups, key=lambda g: (g.members[0].org_name, g.members[0].title, g.group_id))
