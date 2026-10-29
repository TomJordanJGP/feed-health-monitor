"""Shared helpers for building synthetic feed XML in tests (no real feed data or URLs)."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

NOW = datetime(2026, 9, 29, 12, 0, tzinfo=timezone.utc)


def jgp_job(job_id: str, org_id: str = "116", org_name: str = "Surrey County Council", *,
            reference: str = "", title: str = "Social Worker", url: str = "", apply_url: str = "",
            close_date: str = "", description: str = "Role description", old_account_id: str | None = None) -> str:
    return (
        "<job>"
        f"<id><![CDATA[{job_id}]]></id>"
        f"<reference><![CDATA[{reference}]]></reference>"
        f"<title><![CDATA[{title}]]></title>"
        f"<organization_id><![CDATA[{org_id}]]></organization_id>"
        f"<organization_name><![CDATA[{org_name}]]></organization_name>"
        f"<url><![CDATA[{url}]]></url>"
        f"<apply_url><![CDATA[{apply_url}]]></apply_url>"
        f"<close_date><![CDATA[{close_date}]]></close_date>"
        f"<description><![CDATA[{description}]]></description>"
        + (f"<old_account_id><![CDATA[{old_account_id}]]></old_account_id>" if old_account_id is not None else "")
        + "</job>"
    )


def cs_job(job_id: str, jobiqo_id: str = "4204", *, reference: str = "473744",
           title: str = "Prison Officer", url: str = "", close_date: str = "") -> str:
    return (
        "<job>"
        f"<id><![CDATA[{job_id}]]></id>"
        f"<reference><![CDATA[{reference}]]></reference>"
        f"<title><![CDATA[{title}]]></title>"
        f"<jobiqo_id><![CDATA[{jobiqo_id}]]></jobiqo_id>"
        f"<url><![CDATA[{url}]]></url>"
        f"<close_date><![CDATA[{close_date}]]></close_date>"
        "</job>"
    )


def feed_xml(*jobs: str, build: str = "2026-09-29 06:55:41") -> bytes:
    return f"<jobs><lastBuildDate>{build}</lastBuildDate>{''.join(jobs)}</jobs>".encode()


@pytest.fixture
def now() -> datetime:
    return NOW
