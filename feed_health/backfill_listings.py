"""One-off (and re-runnable) copy of Jobiqo's listing history into `export_listings`.

  venv/bin/python -m feed_health.backfill_listings --admin-key ../005_job-performance-dashboard/service_account.json

The collector stores each Jobiqo listing the first time it sees it, from 2026-11-10. The listings
published before that come from JPD's Appcast table, which has polled the same export since
May 2025. Needs a key that can read that table and write the monitor's datasets (the collector
key can do neither). Inserts only listings not already stored, so running it again is harmless.

JPD stored the export's <date> as if it were UTC. It is Central European time, so it is
converted here to match what the collector stores.
"""
from __future__ import annotations

import argparse
from dataclasses import replace

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

from feed_health import store
from feed_health.config import load_settings
from feed_health.feeds import JOBIQO_TZ

SOURCE = "site-monitoring-421401.JPD.t01_feed_appcast"


def backfill_sql(target: str, source: str = SOURCE) -> str:
    return f"""
INSERT INTO `{target}` (entity_id, external_id, published_ts, first_seen_ts)
SELECT a.entity_id, NULLIF(TRIM(a.external_id), ''),
       TIMESTAMP(DATETIME(a.date_posted), '{JOBIQO_TZ.key}'), a.first_seen
FROM `{source}` a
WHERE a.entity_id IS NOT NULL AND a.first_seen IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM `{target}` x WHERE x.entity_id = a.entity_id)"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--admin-key", required=True, help="key that can read the JPD table and write the datasets")
    parser.add_argument("--datasets", nargs="+", default=["feed_health", "feed_health_dev"])
    args = parser.parse_args()

    settings = load_settings(require_urls=False)
    client = bigquery.Client(project=settings.bq_project, location=settings.bq_location,
                             credentials=Credentials.from_service_account_file(args.admin_key, scopes=store.SCOPES))
    for name in args.datasets:
        target = replace(settings, bq_dataset=name).table("export_listings")
        job = client.query(backfill_sql(target))
        job.result()
        print(f"{name}: {job.num_dml_affected_rows or 0:,} listings copied from {SOURCE}")


if __name__ == "__main__":
    main()
