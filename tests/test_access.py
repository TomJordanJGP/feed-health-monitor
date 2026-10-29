"""The staff gate's decisions and its Cloud Identity lookups, with a fake user and session."""
import pytest
import requests

from dashboard.access import NO_SECONDS, YES_SECONDS, AccessCheckError, Directory, decide

GROUP = "jgp-dashboards@jobsgopublic.com"
MEMBER = "staff.member@jobsgopublic.com"


def user(email=MEMBER, verified=True):
    return {"email": email, "email_verified": verified}


def members(*emails):
    return lambda email: email in emails


class TestDecide:
    def test_group_member_is_let_in(self):
        assert decide(user(), members(MEMBER)) == (True, "")

    def test_email_is_matched_case_insensitively(self):
        assert decide(user(" Staff.Member@JobsGoPublic.com "), members(MEMBER))[0]

    def test_string_true_counts_as_verified(self):
        assert decide(user(verified="true"), members(MEMBER))[0]

    @pytest.mark.parametrize("u", [user(verified=False), user(verified=None), {"email": MEMBER}, user(email="")])
    def test_no_verified_email_is_refused(self, u):
        allowed, reason = decide(u, members(MEMBER))
        assert not allowed and "verified email" in reason

    def test_other_domains_are_refused_before_any_lookup(self):
        def must_not_be_called(email):
            raise AssertionError("looked up a non-staff address")
        allowed, reason = decide(user("someone@gmail.com"), must_not_be_called)
        assert not allowed and "@jobsgopublic.com" in reason

    def test_lookalike_domain_is_refused(self):
        assert not decide(user("x@evil-jobsgopublic.com"), members("x@evil-jobsgopublic.com"))[0]

    def test_staff_outside_the_group_are_refused(self):
        allowed, reason = decide(user("new.starter@jobsgopublic.com"), members(MEMBER))
        assert not allowed and "staff group" in reason


class FakeResponse:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body or {}

    def json(self):
        return self._body


class FakeSession:
    """Answers groups:lookup with `group_status`, and memberships:lookup from `memberships`."""

    def __init__(self, memberships=None, group_status=200):
        self.memberships, self.group_status, self.calls = memberships or {}, group_status, []

    def get(self, url, params, timeout):
        self.calls.append(url.rsplit("/", 1)[-1])
        if url.endswith("groups:lookup"):
            return FakeResponse(self.group_status, {"name": "groups/abc123"})
        assert url.endswith("groups/abc123/memberships:lookup")
        status = self.memberships.get(params["memberKey.id"], 403)
        if isinstance(status, Exception):
            raise status
        return FakeResponse(status, {"name": "groups/abc123/memberships/1"})


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def test_member_lookup_and_caching():
    session, clock = FakeSession({MEMBER: 200}), Clock()
    directory = Directory(GROUP, session, clock)
    assert directory.is_member(MEMBER) and directory.is_member(MEMBER)
    assert session.calls == ["groups:lookup", "memberships:lookup"]       # second answer from cache
    clock.now += YES_SECONDS + 1
    assert directory.is_member(MEMBER)
    assert session.calls == ["groups:lookup", "memberships:lookup", "memberships:lookup"]   # group name kept


def test_non_members_are_rechecked_sooner():
    session, clock = FakeSession({"a@jobsgopublic.com": 403, "b@jobsgopublic.com": 404}), Clock()
    directory = Directory(GROUP, session, clock)
    assert not directory.is_member("a@jobsgopublic.com") and not directory.is_member("b@jobsgopublic.com")
    session.memberships["a@jobsgopublic.com"] = 200      # added to the group
    clock.now += NO_SECONDS + 1
    assert directory.is_member("a@jobsgopublic.com")


@pytest.mark.parametrize("session", [FakeSession(group_status=403), FakeSession({MEMBER: 500}),
                                     FakeSession({MEMBER: requests.ConnectionError("down")})])
def test_failures_raise_rather_than_answer(session):
    with pytest.raises(AccessCheckError):
        Directory(GROUP, session).is_member(MEMBER)
