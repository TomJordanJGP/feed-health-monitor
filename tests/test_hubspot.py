"""The HubSpot company pull against a fake session: paging, retries and failures."""
import pytest
import requests

from feed_health.hubspot import Company, HubSpotError, fetch_companies

TOKEN = "test-token-not-real"


class FakeResponse:
    def __init__(self, status, body=None, headers=None):
        self.status_code, self._body, self.headers = status, body, headers or {}

    def json(self):
        if self._body is None:
            raise ValueError("no JSON")
        return self._body


class FakeSession:
    def __init__(self, *responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, headers, params, timeout):
        self.calls.append({"url": url, "headers": headers, "params": dict(params)})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def page(*companies, after=None):
    body = {"results": [{"id": cid, "properties": props} for cid, props in companies]}
    if after:
        body["paging"] = {"next": {"after": after}}
    return FakeResponse(200, body)


ONE = page(("11", {"name": "Sefton Council", "jobiqo_id": "934.0", "ats_org_id": None, "lifecyclestage": "customer"}))


def fetch(session):
    sleeps = []
    return fetch_companies(TOKEN, session, sleep=sleeps.append), sleeps


def test_pages_through_every_company():
    session = FakeSession(
        page(("11", {"name": " Sefton Council ", "jobiqo_id": "934.0", "ats_org_id": "5512", "lifecyclestage": "customer"}),
             after="100"),
        page(("12", {"name": None, "jobiqo_id": None, "ats_org_id": None, "lifecyclestage": None})))
    companies, sleeps = fetch(session)
    assert companies == [Company("11", "Sefton Council", "934", "5512", "customer"), Company("12", "", "", "", "")]
    assert sleeps == []
    first, second = (c["params"] for c in session.calls)
    assert first == {"limit": 100, "properties": "name,jobiqo_id,ats_org_id,lifecyclestage", "archived": "false"}
    assert second["after"] == "100"
    assert session.calls[0]["headers"]["Authorization"] == f"Bearer {TOKEN}"


def test_rate_limit_waits_as_told_then_continues():
    companies, sleeps = fetch(FakeSession(FakeResponse(429, {"category": "RATE_LIMITS"}, {"Retry-After": "7"}), ONE))
    assert sleeps == [7.0] and len(companies) == 1


def test_server_errors_and_network_failures_back_off_then_recover():
    session = FakeSession(FakeResponse(502), requests.ConnectionError("reset"), FakeResponse(503), ONE)
    companies, sleeps = fetch(session)
    assert sleeps == [2.0, 4.0, 8.0] and len(companies) == 1


def test_a_long_retry_after_is_capped():
    _, sleeps = fetch(FakeSession(FakeResponse(429, None, {"Retry-After": "600"}), ONE))
    assert sleeps == [30]


def test_bad_token_fails_at_once_without_echoing_it():
    session = FakeSession(FakeResponse(401, {"category": "INVALID_AUTHENTICATION", "message": f"bad {TOKEN}"}))
    with pytest.raises(HubSpotError) as exc:
        fetch(session)
    assert str(exc.value) == "HubSpot HTTP 401 (INVALID_AUTHENTICATION)"
    assert len(session.calls) == 1 and TOKEN not in str(exc.value)


def test_gives_up_after_three_retries():
    session = FakeSession(*[FakeResponse(500)] * 4)
    with pytest.raises(HubSpotError, match=r"HubSpot HTTP 500 \(after 4 attempts\)"):
        fetch(session)


def test_an_empty_list_is_an_error_not_an_empty_snapshot():
    with pytest.raises(HubSpotError, match="no companies"):
        fetch(FakeSession(page()))


def test_a_page_that_isnt_json_is_an_error():
    with pytest.raises(HubSpotError, match="isn't JSON"):
        fetch(FakeSession(FakeResponse(200, None)))
