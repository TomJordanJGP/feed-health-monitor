"""HubSpot CRM: the full company list, read 100 companies per request (the API's page limit).

A full pull is ~90 requests (~30 s). Every company is fetched, not just changes, so companies
deleted or merged in HubSpot drop out of the snapshot on their own. The repo's logs are public:
errors carry HTTP statuses and HubSpot's error category only, never names or the token.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

import requests

from feed_health.feeds import USER_AGENT, clean_id

API = "https://api.hubapi.com/crm/v3/objects/companies"
PROPERTIES = ("name", "jobiqo_id", "ats_org_id", "lifecyclestage")
PAGE_SIZE = 100
RETRIES = 3              # per page, for rate limits (429), server errors (5xx) and network failures
MAX_WAIT_SECONDS = 30    # cap on a Retry-After, so a run stays well inside the Action's timeout
TIMEOUT = (10, 60)       # connect, read (seconds)


class HubSpotError(RuntimeError):
    """The company list couldn't be read in full; nothing should be written from it."""


@dataclass(frozen=True)
class Company:
    hubspot_id: str
    name: str
    jobiqo_id: str          # "Jobiqo Org ID": the feeds' organization_id / Civil Service jobiqo_id
    ats_org_id: str         # "ATS Org ID": the feeds' old_account_id
    lifecycle_stage: str    # HubSpot's internal value, e.g. "customer"; "" when not set


def fetch_companies(token: str, session: requests.Session | None = None,
                    sleep: Callable[[float], None] = time.sleep) -> list[Company]:
    """Every company that isn't archived. Raises HubSpotError on any failure, and on an empty
    list, which would otherwise replace the snapshot with nothing."""
    session = session or requests.Session()
    headers = {"Authorization": f"Bearer {token}", "User-Agent": USER_AGENT}
    companies: list[Company] = []
    after = None
    while True:
        params = {"limit": PAGE_SIZE, "properties": ",".join(PROPERTIES), "archived": "false"}
        if after:
            params["after"] = after
        body = _get_page(session, headers, params, sleep)
        for c in body.get("results") or []:
            p = c.get("properties") or {}
            companies.append(Company(hubspot_id=str(c["id"]), name=(p.get("name") or "").strip(),
                                     jobiqo_id=clean_id(p.get("jobiqo_id")),
                                     ats_org_id=clean_id(p.get("ats_org_id")),
                                     lifecycle_stage=(p.get("lifecyclestage") or "").strip()))
        after = ((body.get("paging") or {}).get("next") or {}).get("after")
        if not after:
            break
    if not companies:
        raise HubSpotError("HubSpot returned no companies")
    return companies


def _get_page(session: requests.Session, headers: dict, params: dict, sleep: Callable[[float], None]) -> dict:
    problem = ""
    for attempt in range(RETRIES + 1):
        wait = 2.0 ** (attempt + 1)
        try:
            response = session.get(API, headers=headers, params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            problem = f"HubSpot request failed: {type(exc).__name__}"
        else:
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError:
                    raise HubSpotError("HubSpot returned a page that isn't JSON") from None
            problem = f"HubSpot HTTP {response.status_code}{_category(response)}"
            if response.status_code != 429 and response.status_code < 500:
                raise HubSpotError(problem)  # e.g. 401: an expired or revoked token won't recover
            wait = _retry_after(response) or wait
        if attempt < RETRIES:
            sleep(min(wait, MAX_WAIT_SECONDS))
    raise HubSpotError(f"{problem} (after {RETRIES + 1} attempts)")


def _retry_after(response: requests.Response) -> float | None:
    try:
        return float(response.headers.get("Retry-After", ""))
    except ValueError:
        return None


def _category(response: requests.Response) -> str:
    """HubSpot's error category (e.g. INVALID_AUTHENTICATION); never the message, which can echo input."""
    try:
        category = response.json().get("category")
    except (ValueError, AttributeError):
        return ""
    return f" ({category})" if isinstance(category, str) and category else ""
