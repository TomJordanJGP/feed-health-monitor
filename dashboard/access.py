"""Staff-only access for a Streamlit dashboard: Google sign-in (st.login), then membership of a
Google Group, checked with the app's own service account (a plain member of the group).

The Feed Health Monitor and the Job Performance Dashboard use this same file; keep the copies
identical. Secrets (Streamlit Cloud: the app's settings; locally: .streamlit/secrets.toml):

  [auth]                  the Google sign-in client: redirect_uri, cookie_secret, client_id,
                          client_secret, server_metadata_url
  [access]                group = "jgp-dashboards@jobsgopublic.com"
  [gcp_service_account]   the app's service-account key; the account must be in the group

Anything missing or failing refuses access. Membership answers are cached for 10 minutes (yes)
or 1 minute (no), so removing someone from the group takes effect within 10 minutes.
"""
from __future__ import annotations

import sys
import time
from collections.abc import Callable, Mapping

import requests
import streamlit as st
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials

DOMAIN = "jobsgopublic.com"
API = "https://cloudidentity.googleapis.com/v1"
SCOPES = ["https://www.googleapis.com/auth/cloud-identity.groups.readonly"]
YES_SECONDS, NO_SECONDS = 600, 60
TIMEOUT = 10


class AccessCheckError(RuntimeError):
    """Membership couldn't be checked (configuration or API problem): refuse rather than guess."""


def decide(user: Mapping, is_member: Callable[[str], bool]) -> tuple[bool, str]:
    """(allowed, reason shown when refused) for a signed-in user."""
    email = str(user.get("email") or "").strip().lower()
    if not email or user.get("email_verified") not in (True, "true"):
        return False, "Your Google account didn't share a verified email address."
    if not email.endswith("@" + DOMAIN):
        return False, f"Sign in with your @{DOMAIN} Google account."
    if not is_member(email):
        return False, "Your account isn't in the dashboards' staff group. Ask Tom Jordan to add you."
    return True, ""


class Directory:
    """Group membership through the Cloud Identity API. memberships:lookup works on every
    Workspace edition (checkTransitiveMembership needs a premium one), so only direct members of
    the group count, not members of groups inside it."""

    def __init__(self, group: str, session, clock: Callable[[], float] = time.monotonic):
        self.group, self._session, self._clock = group, session, clock
        self._group_name: str | None = None
        self._answers: dict[str, tuple[bool, float]] = {}

    def is_member(self, email: str) -> bool:
        cached = self._answers.get(email)
        if cached and cached[1] > self._clock():
            return cached[0]
        if self._group_name is None:
            response = self._get("groups:lookup", {"groupKey.id": self.group})
            if response.status_code != 200:
                raise AccessCheckError(f"group lookup: HTTP {response.status_code}")
            self._group_name = response.json()["name"]
        response = self._get(f"{self._group_name}/memberships:lookup", {"memberKey.id": email})
        if response.status_code == 200:
            member = True
        elif response.status_code in (403, 404):  # Cloud Identity answers 403 for "no such membership"
            member = False
        else:
            raise AccessCheckError(f"membership lookup: HTTP {response.status_code}")
        self._answers[email] = (member, self._clock() + (YES_SECONDS if member else NO_SECONDS))
        return member

    def _get(self, path: str, params: dict):
        try:
            return self._session.get(f"{API}/{path}", params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            raise AccessCheckError(f"Cloud Identity request failed: {type(exc).__name__}") from None


@st.cache_resource(show_spinner=False)
def _directory(group: str) -> Directory:
    info = _secret("gcp_service_account")
    if not info:
        raise AccessCheckError("no [gcp_service_account] secret")
    credentials = Credentials.from_service_account_info(dict(info), scopes=SCOPES)
    return Directory(group, AuthorizedSession(credentials))


def _secret(name: str):
    try:
        return st.secrets.get(name)
    except (FileNotFoundError, st.errors.StreamlitSecretNotFoundError):
        return None


def _refuse(title: str, message: str, sign_out: bool) -> None:
    st.title(title)
    st.write(message)
    if sign_out and st.button("Sign out"):
        st.logout()
    st.stop()


def require_staff() -> None:
    """Call before the app reads any data. Unless the viewer is signed in with a jobsgopublic.com
    account in the staff group, it shows a sign-in or refusal page and stops the script."""
    group = (_secret("access") or {}).get("group")
    if not _secret("auth") or not group:
        _refuse("Sign-in isn't set up", "This app's [auth] or [access] secrets are missing.", sign_out=False)
    if not st.user.is_logged_in:
        st.title("Sign in")
        st.write("This dashboard is for Jobs Go Public staff.")
        if st.button("Sign in with Google", type="primary"):
            st.login()
        st.stop()
    try:
        allowed, reason = decide(st.user, _directory(group).is_member)
    except AccessCheckError as exc:
        print(f"access check failed: {exc}", file=sys.stderr)  # the app's logs; never shown to viewers
        allowed, reason = False, "Your access couldn't be checked just now. Try again in a minute."
    if not allowed:
        _refuse("No access", f"Signed in as {st.user.get('email')}. {reason}", sign_out=True)
