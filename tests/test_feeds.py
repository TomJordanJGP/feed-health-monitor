from datetime import datetime, timezone

import pytest

from feed_health import feeds
from feed_health.feeds import ExportOrg, FeedParseError, clean_id, norm_url, parse_feed, parse_feed_datetime, ref_is_usable
from tests.conftest import cs_job, feed_xml, jgp_job

UTC = timezone.utc


class TestDates:
    def test_month_first_never_day_first(self, now):
        assert parse_feed_datetime("09/03/2026 23:00:00", now) == datetime(2026, 9, 3, 23, 0, tzinfo=UTC)

    def test_offset_converted_to_utc(self, now):
        assert parse_feed_datetime("2026-09-14 12:48:00 +0100", now) == datetime(2026, 9, 14, 11, 48, tzinfo=UTC)

    def test_last_build_date_format_is_naive_utc(self, now):
        assert parse_feed_datetime("2026-09-28 06:55:41", now) == datetime(2026, 9, 28, 6, 55, 41, tzinfo=UTC)

    @pytest.mark.parametrize("raw", ["01/01/0001 00:00:00", "01/01/2099 00:00:00",
                                     "2925-09-05 00:00:00 +0000", "05/31/3026 00:00:00", "12/31/1999 00:00:00"])
    def test_junk_and_placeholder_years_are_missing(self, raw, now):
        assert parse_feed_datetime(raw, now) is None

    def test_three_years_ahead_is_still_a_date(self, now):
        assert parse_feed_datetime("06/30/2029 00:00:00", now) is not None

    @pytest.mark.parametrize("raw", ["", "   ", None, "tomorrow", "2026/09/03"])
    def test_blank_or_unparseable_is_missing(self, raw, now):
        assert parse_feed_datetime(raw, now) is None


class TestUrls:
    def test_strips_feed_routing_and_tracking_params(self):
        a = norm_url("https://Careers.Example.gov.uk/job/?j=1818&jobboard=jgp&ATSI=123&utm_source=x#apply")
        assert a == "https://careers.example.gov.uk/job?j=1818"

    def test_only_the_vacancyposter_value_of_c_is_stripped(self):
        assert norm_url("https://x.example/v?c=vacancyposter&j=1") == "https://x.example/v?j=1"
        assert norm_url("https://x.example/v?c=other&j=1") == "https://x.example/v?c=other&j=1"

    def test_amp_entity_unescaped_but_other_entities_left_alone(self):
        assert norm_url("https://x.example/v?a=1&amp;region=north") == "https://x.example/v?a=1&region=north"
        assert "®" not in norm_url("https://x.example/v?a=1&region=north")

    def test_params_are_order_independent(self):
        assert norm_url("https://x.example/v?b=2&a=1") == norm_url("https://x.example/v?a=1&b=2")

    @pytest.mark.parametrize("raw", ["", "   ", None])
    def test_empty(self, raw):
        assert norm_url(raw) == ""


class TestReferences:
    @pytest.mark.parametrize("ref", ["External", "n/a", "TBC", "-", "0", "ab", "abcdef"])
    def test_placeholders_and_digitless_refs_unusable(self, ref):
        assert not ref_is_usable(ref)

    @pytest.mark.parametrize("ref", ["coventrycc/TP/237/13214", "473744", "req23583", " 12 34 "])
    def test_real_refs_usable(self, ref):
        assert ref_is_usable(ref)


class TestParseFeed:
    def test_jgp_and_civil_service_schemas(self, now):
        parsed = parse_feed(feed_xml(
            jgp_job("a1", reference="R-001", url="https://x.example/a?jobboard=jgp", close_date="10/11/2026 23:00:00"),
            cs_job("c1", close_date="10/29/2026 23:55:00"),
        ), now)
        assert parsed.build_date == datetime(2026, 9, 29, 6, 55, 41, tzinfo=UTC)
        a, c = parsed.jobs
        assert (a.org_key, a.org_name, a.url_norm) == ("jgp:116", "Surrey County Council", "https://x.example/a")
        assert a.close_date == datetime(2026, 10, 11, 23, 0, tzinfo=UTC)
        assert (c.org_key, c.org_name) == ("cs:4204", "")

    def test_url_falls_back_to_apply_url(self, now):
        (job,) = parse_feed(feed_xml(jgp_job("f1", url="", apply_url="https://apply.example/f1")), now).jobs
        assert job.url_norm == "https://apply.example/f1"

    def test_names_normalised(self, now):
        (job,) = parse_feed(feed_xml(jgp_job("d1", org_name="Drapers’­  Multi-Academy Trust")), now).jobs
        assert job.org_name == "Drapers’ Multi-Academy Trust"

    def test_duplicate_ids_kept_once_and_counted(self, now):
        parsed = parse_feed(feed_xml(jgp_job("a1"), jgp_job("a1", title="Dup"), jgp_job("a2")), now)
        assert [j.job_id for j in parsed.jobs] == ["a1", "a2"]
        assert parsed.duplicate_ids == 1

    def test_valid_empty_feed_is_not_an_error(self, now):
        parsed = parse_feed(feed_xml(), now)
        assert parsed.jobs == () and parsed.build_date is not None

    def test_truncated_file_is_an_error(self, now):
        data = feed_xml(jgp_job("a1"), jgp_job("a2"))
        with pytest.raises(FeedParseError):
            parse_feed(data[: len(data) // 2], now)

    def test_wrong_root_is_an_error(self, now):
        with pytest.raises(FeedParseError, match="expected <jobs>"):
            parse_feed(b"<html><body>Service unavailable</body></html>", now)

    def test_non_xml_is_an_error(self, now):
        with pytest.raises(FeedParseError):
            parse_feed(b"", now)

    def test_reorder_only_rebuild_has_same_hash(self, now):
        one = parse_feed(feed_xml(jgp_job("a1"), jgp_job("a2"), build="2026-09-29 06:55:41"), now)
        two = parse_feed(feed_xml(jgp_job("a2"), jgp_job("a1"), build="2026-09-29 09:55:41"), now)
        assert one.content_hash == two.content_hash

    def test_description_only_change_has_same_hash(self, now):
        one = parse_feed(feed_xml(jgp_job("a1", description="old")), now)
        two = parse_feed(feed_xml(jgp_job("a1", description="new")), now)
        assert one.content_hash == two.content_hash

    def test_close_date_change_changes_hash(self, now):
        one = parse_feed(feed_xml(jgp_job("a1", close_date="10/01/2026 23:00:00")), now)
        two = parse_feed(feed_xml(jgp_job("a1", close_date="10/08/2026 23:00:00")), now)
        assert one.content_hash != two.content_hash

    def test_old_account_id_is_kept_but_not_hashed(self, now):
        one = parse_feed(feed_xml(jgp_job("a1")), now)
        two = parse_feed(feed_xml(jgp_job("a1", old_account_id=" 5512.0 ")), now)
        assert one.content_hash == two.content_hash   # else every feed's next build would look changed
        assert (one.jobs[0].ats_org_id, two.jobs[0].ats_org_id) == ("", "5512")


@pytest.mark.parametrize("raw, cleaned", [("934", "934"), (" 934.0 ", "934"), ("", ""), (None, ""), ("A1.0", "A1.0")])
def test_clean_id(raw, cleaned):
    assert clean_id(raw) == cleaned


def test_jobiqo_export_ids_and_companies():
    data = (b"<jobs>"
            b"<job><id>1</id><external_id><![CDATA[a1]]></external_id><organization_id>31727</organization_id>"
            b"<company><![CDATA[Surrey County Council]]></company></job>"
            b"<job><id>2</id><external_id></external_id><organization_id>31727</organization_id><company>Surrey County Council</company></job>"
            b"<job><id>3</id><external_id>b2</external_id><organization_id></organization_id><company>No Id Org</company></job>"
            b"</jobs>")
    export = feeds.parse_jobiqo_export(data)
    assert export.ids == {"a1", "b2"} and export.jobs == 3
    assert export.organizations == 2   # one org id shared by two jobs, plus one org known only by name
    assert export.company_by_id == {"a1": "Surrey County Council", "b2": "No Id Org"}
    assert export.orgs == {"31727": ExportOrg("31727", "Surrey County Council", jobs=2, site_jobs=1),
                           "No Id Org": ExportOrg("No Id Org", "No Id Org", jobs=1, site_jobs=0)}
    assert export.org_by_id == {"a1": "31727", "b2": "No Id Org"}   # feed job ids -> the export's numbering


def test_feed_map():
    data = (b"<items><lastBuildDate>2026-09-28 06:55:41</lastBuildDate>"
            b"<item><name>Jgp Ats Feed</name><url>https://bucket.example/jgp_scrapes/jgp_ats_feed.xml</url></item>"
            b"<item><name>Broken</name><url></url></item></items>")
    (entry,) = feeds.parse_feed_map(data)
    assert (entry.name, entry.file) == ("Jgp Ats Feed", "jgp_ats_feed.xml")
