"""Fetching and parsing feed XML, plus the normalisers that cross-feed matching relies on.

All source feeds share one layout, <jobs><lastBuildDate/><job/>…</jobs>, in two schemas:
  * JGP (ATS, scrape, backfill, Tribepad): <organization_id> and <organization_name>, and on
    some feeds <old_account_id> (the employer's ATS account; HubSpot's "ATS Org ID")
  * Civil Service: <jobiqo_id> and no employer name
<organization_id> and <jobiqo_id> are the same Jobiqo numbering (HubSpot's "Jobiqo Org ID").
Jobiqo's Appcast export lists what is live on the board: <job><external_id>, which is the
source feed's <id>, plus <company> and an <organization_id> in the export's OWN numbering.
"""
from __future__ import annotations

import email.utils
import hashlib
import io
import re
import unicodedata
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests

USER_AGENT = "jgp-feed-health-monitor/1.0"
TIMEOUT = (10, 120)  # connect, read (seconds); the largest feed is ~30 MB


class FeedParseError(ValueError):
    """The body is not a complete feed document (truncated, not XML, or the wrong root)."""


# --------------------------------------------------------------------------- fetching

@dataclass(frozen=True)
class FetchResult:
    status: int                    # HTTP status; 0 when the request itself failed
    body: bytes | None             # None on 304 or on error
    etag: str | None
    generation: str | None         # GCS object generation (changes on every rewrite)
    last_modified: datetime | None
    error: str | None

    @property
    def unchanged(self) -> bool:
        return self.status == 304


def fetch(url: str, etag: str | None = None, session: requests.Session | None = None) -> FetchResult:
    """GET, conditional on the previous ETag when there is one (GCS and Jobiqo answer 304).
    Error text never includes the URL: feed URLs are private and errors are logged."""
    headers = {"User-Agent": USER_AGENT}
    if etag:
        headers["If-None-Match"] = etag
    try:
        resp = (session or requests).get(url, headers=headers, timeout=TIMEOUT)
    except requests.RequestException as exc:
        return FetchResult(0, None, None, None, None, f"request failed: {type(exc).__name__}")
    meta = dict(status=resp.status_code, etag=resp.headers.get("ETag"),
                generation=resp.headers.get("x-goog-generation"),
                last_modified=_http_date(resp.headers.get("Last-Modified")))
    if resp.status_code == 304:
        return FetchResult(body=None, error=None, **meta)
    if resp.status_code != 200:
        return FetchResult(body=None, error=f"HTTP {resp.status_code}", **meta)
    return FetchResult(body=resp.content, error=None, **meta)


def _http_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return email.utils.parsedate_to_datetime(value).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- normalisers

_DATE_FORMATS = ("%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S")
MIN_YEAR = 2000
MAX_YEARS_AHEAD = 3  # 2099 / 2925 / 3026 are "no closing date" placeholders, not dates


def parse_feed_datetime(raw: str | None, now: datetime) -> datetime | None:
    """Feed timestamps: month-first MM/DD/YYYY (naive UTC; never day-first) or ISO with an
    offset. Years outside 2000..now+3 are placeholders or junk and count as missing.
    The year is checked before any timezone conversion, so year 0001 can't overflow."""
    raw = (raw or "").strip()
    if not raw:
        return None
    for fmt in _DATE_FORMATS:
        try:
            parsed = datetime.strptime(raw, fmt)
        except ValueError:
            continue
        if not MIN_YEAR <= parsed.year <= now.year + MAX_YEARS_AHEAD:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


# Query parameters that differ between copies of the same advert: feed routing tags
# (jobboard=, ATSI=), tracking (utm_*), and the ATS multiposter tag c=vacancyposter.
# Every other parameter is kept: many ATSs identify the job in the query string (j=1818).
_STRIP_KEYS = frozenset({"jobboard", "atsi"})
_STRIP_PREFIXES = ("utm_",)
_STRIP_PAIRS = frozenset({("c", "vacancyposter")})


def _keep_param(key: str, value: str) -> bool:
    k = key.lower()
    return (k not in _STRIP_KEYS and not k.startswith(_STRIP_PREFIXES)
            and (k, value.lower()) not in _STRIP_PAIRS)


def norm_url(raw: str | None) -> str:
    """Comparable form of an advert URL; '' when there is none. Only '&amp;' is unescaped:
    a full HTML unescape would turn '&region=' into '®ion='."""
    u = (raw or "").strip().replace("&amp;", "&")
    if not u:
        return ""
    try:
        parts = urlsplit(u)
        query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if _keep_param(k, v)]
        return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"),
                           urlencode(sorted(query)), ""))
    except ValueError:  # e.g. a malformed IPv6 host
        return u.lower()


_PLACEHOLDER_REFS = frozenset({"external", "internal", "na", "n/a", "tbc", "tba", "none", "null", "-", "0", "ref"})


def norm_ref(raw: str | None) -> str:
    return re.sub(r"\s+", "", raw or "").casefold()


def ref_is_usable(raw: str | None) -> bool:
    """A reference can identify a vacancy only if it has 3+ characters including a digit and
    isn't a placeholder word. Uniqueness is checked by the caller, within one feed version."""
    ref = norm_ref(raw)
    return len(ref) >= 3 and any(ch.isdigit() for ch in ref) and ref not in _PLACEHOLDER_REFS


def display_name(raw: str | None) -> str:
    """NFKC, soft hyphens removed, whitespace collapsed (e.g. "Drapers’­ Multi-Academy Trust")."""
    text = unicodedata.normalize("NFKC", raw or "").replace("­", "")
    return re.sub(r"\s+", " ", text).strip()


def name_key(raw: str | None) -> str:
    """Identity for employer names across feeds and ID namespaces."""
    return display_name(raw).casefold()


def clean_id(raw: str | None) -> str:
    """An ID as a stripped string; a numeric ID written as a float ("934.0") loses the ".0"."""
    value = (raw or "").strip()
    return value[:-2] if value.endswith(".0") and value[:-2].isdigit() else value


# --------------------------------------------------------------------------- parsing

@dataclass(frozen=True)
class Job:
    job_id: str
    reference: str
    title: str
    org_key: str            # "jgp:<organization_id>", "cs:<jobiqo_id>", or "none:"
    org_name: str           # "" for Civil Service until named from the Jobiqo export
    url_norm: str
    close_date: datetime | None
    ats_org_id: str = ""    # <old_account_id>; kept out of the content hash (added 2026-10-29)


@dataclass(frozen=True)
class ParsedFeed:
    build_date: datetime | None      # the feed's own <lastBuildDate> (naive UTC in the feed)
    jobs: tuple[Job, ...]            # unique by job_id; later duplicates are dropped
    duplicate_ids: int
    content_hash: str                # over the raw fields; equal hash = no change worth a version


# The fields the content hash covers (raw), so a rebuild that only reorders jobs or edits
# descriptions is not a new version. <old_account_id> is stored too but deliberately left out:
# adding it would have made every feed's next build look like a change.
_JOB_FIELDS = ("id", "reference", "title", "organization_id", "organization_name", "jobiqo_id",
               "url", "apply_url", "close_date")


def parse_feed(data: bytes, now: datetime) -> ParsedFeed:
    """Stream-parse a source feed. Raises FeedParseError for anything that isn't a complete
    <jobs> document, so a truncated upload is reported as an error, never as a version with
    fewer jobs. A valid document with no jobs is a genuine empty feed."""
    build_raw: str | None = None
    jobs: list[Job] = []
    raw_rows: list[tuple[str, ...]] = []
    seen: set[str] = set()
    duplicates = 0
    root = None
    depth = 0
    try:
        for event, elem in ET.iterparse(io.BytesIO(data), events=("start", "end")):
            if event == "start":
                if root is None:
                    root = elem
                    if root.tag != "jobs":
                        raise FeedParseError(f"root element is <{root.tag}>, expected <jobs>")
                depth += 1
                continue
            depth -= 1
            if depth != 1:
                continue
            if elem.tag == "lastBuildDate":
                build_raw = (elem.text or "").strip()
            elif elem.tag == "job":
                raw = {f: (elem.findtext(f) or "").strip() for f in _JOB_FIELDS}
                raw_rows.append(tuple(raw[f] for f in _JOB_FIELDS))
                job = _job(raw, now, elem.findtext("old_account_id"))
                if job.job_id in seen:
                    duplicates += 1
                else:
                    seen.add(job.job_id)
                    jobs.append(job)
            root.clear()
    except ET.ParseError as exc:
        raise FeedParseError(f"invalid XML: {exc}") from None
    if root is None:
        raise FeedParseError("empty document")
    return ParsedFeed(
        build_date=parse_feed_datetime(build_raw, now) if build_raw else None,
        jobs=tuple(jobs),
        duplicate_ids=duplicates,
        content_hash=_hash_rows(raw_rows),
    )


def _job(raw: dict[str, str], now: datetime, old_account_id: str | None = None) -> Job:
    if raw["organization_id"]:
        org_key, org_name = f"jgp:{raw['organization_id']}", display_name(raw["organization_name"])
    elif raw["jobiqo_id"]:
        org_key, org_name = f"cs:{raw['jobiqo_id']}", ""
    else:
        org_key, org_name = "none:", ""
    job_id = raw["id"] or "noid:" + hashlib.md5(
        "\x1f".join((raw["url"], raw["reference"], raw["title"])).encode()).hexdigest()
    return Job(
        job_id=job_id,
        reference=raw["reference"],
        title=display_name(raw["title"]),
        org_key=org_key,
        org_name=org_name,
        url_norm=norm_url(raw["url"]) or norm_url(raw["apply_url"]),
        close_date=parse_feed_datetime(raw["close_date"], now),
        ats_org_id=clean_id(old_account_id),
    )


def _hash_rows(rows: list[tuple[str, ...]]) -> str:
    digest = hashlib.sha256()
    for row in sorted(rows):
        digest.update("\x1f".join(row).encode())
        digest.update(b"\x1e")
    return digest.hexdigest()


@dataclass(frozen=True)
class ExportOrg:
    """An employer as Jobiqo's export numbers it (not the feeds' numbering)."""
    org_id: str                    # export <organization_id>, or the company name if it has none
    company: str
    jobs: int                      # all its jobs live on the board
    site_jobs: int                 # of which created on Jobiqo directly (no external_id)


@dataclass(frozen=True)
class JobiqoExport:
    ids: frozenset[str]            # external_ids live on Jobiqo (= source feed <id>s)
    company_by_id: dict[str, str]  # external_id -> Jobiqo company name
    jobs: int                      # all <job> elements, including site-created ones
    organizations: int = 0         # distinct Jobiqo organization_id (company name if an id is missing)
    orgs: dict[str, ExportOrg] = field(default_factory=dict)     # by org_id
    org_by_id: dict[str, str] = field(default_factory=dict)      # external_id -> org_id


def parse_jobiqo_export(data: bytes) -> JobiqoExport:
    ids: set[str] = set()
    company: dict[str, str] = {}
    org_company: dict[str, str] = {}
    org_jobs: dict[str, int] = {}
    org_site_jobs: dict[str, int] = {}
    org_by_id: dict[str, str] = {}
    count = 0
    root = None
    depth = 0
    try:
        for event, elem in ET.iterparse(io.BytesIO(data), events=("start", "end")):
            if event == "start":
                if root is None:
                    root = elem
                    if root.tag != "jobs":
                        raise FeedParseError(f"root element is <{root.tag}>, expected <jobs>")
                depth += 1
                continue
            depth -= 1
            if depth == 1 and elem.tag == "job":
                count += 1
                name = display_name(elem.findtext("company"))
                org = clean_id(elem.findtext("organization_id")) or name
                ext = (elem.findtext("external_id") or "").strip()
                if org:
                    org_company[org] = org_company.get(org) or name
                    org_jobs[org] = org_jobs.get(org, 0) + 1
                    if not ext:
                        org_site_jobs[org] = org_site_jobs.get(org, 0) + 1
                if ext:  # site-created jobs have no external_id
                    ids.add(ext)
                    company[ext] = name
                    if org:
                        org_by_id[ext] = org
                root.clear()
    except ET.ParseError as exc:
        raise FeedParseError(f"invalid XML: {exc}") from None
    if root is None:
        raise FeedParseError("empty document")
    orgs = {o: ExportOrg(o, org_company[o], org_jobs[o], org_site_jobs.get(o, 0)) for o in org_jobs}
    return JobiqoExport(ids=frozenset(ids), company_by_id=company, jobs=count, organizations=len(orgs),
                        orgs=orgs, org_by_id=org_by_id)


@dataclass(frozen=True)
class FeedMapEntry:
    name: str
    url: str

    @property
    def file(self) -> str:
        return file_name(self.url)


def parse_feed_map(data: bytes) -> list[FeedMapEntry]:
    """feed_map.xml: <items><lastBuildDate/><item><name/><url/></item>…</items>."""
    try:
        root = ET.fromstring(data)
    except ET.ParseError as exc:
        raise FeedParseError(f"invalid XML: {exc}") from None
    if root.tag != "items":
        raise FeedParseError(f"root element is <{root.tag}>, expected <items>")
    entries = []
    for item in root.findall("item"):
        url = (item.findtext("url") or "").strip()
        if url:
            entries.append(FeedMapEntry(name=(item.findtext("name") or "").strip(), url=url))
    return entries


def file_name(url: str) -> str:
    return urlsplit(url).path.rsplit("/", 1)[-1]
