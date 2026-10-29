"""One-time (and re-runnable) setup of the BigQuery side.

  venv/bin/python -m feed_health.setup --admin-key ../005_job-performance-dashboard/service_account.json

1. With an ADMIN key: create each dataset (EU) if missing and grant the collector service
   account Data Editor on it, and nothing else. The collector key can't create datasets.
   `--dashboard-reader EMAIL` also grants the dashboard's service account read-only access
   (Data Viewer) on each dataset, so the app can't change the data. The hosted app reads
   feed_health; local runs use the same key (.streamlit/secrets.toml) and may read dev.
2. With the COLLECTOR key (from .env): create the tables and (re)create the views. Doing this
   as the collector proves its permissions work before anything is scheduled.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import replace

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

from feed_health import store
from feed_health.config import load_settings

DESCRIPTIONS = {
    "feed_health": "Feed Health Monitor (production). Written only by the scheduled GitHub Action.",
    "feed_health_dev": "Feed Health Monitor (development). Local runs write here.",
}


def grant(admin: bigquery.Client, ds: bigquery.Dataset, role: str, email: str) -> None:
    """Give a service account a dataset-level role (WRITER = Data Editor, READER = Data Viewer), once."""
    if any(e.entity_id == email and e.role == role for e in ds.access_entries):
        print(f"{ds.dataset_id}: {email} already has {role}")
        return
    ds.access_entries = [*ds.access_entries, bigquery.AccessEntry(role=role, entity_type="userByEmail", entity_id=email)]
    admin.update_dataset(ds, ["access_entries"])
    print(f"{ds.dataset_id}: granted {role} to {email}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--admin-key", required=True, help="service-account key with BigQuery Admin")
    parser.add_argument("--datasets", nargs="+", default=["feed_health", "feed_health_dev"])
    parser.add_argument("--dashboard-reader", metavar="EMAIL",
                        help="the dashboard's service account: read-only access on each dataset")
    args = parser.parse_args()

    settings = load_settings(require_urls=False)
    collector_email = json.loads(settings.credentials_path.read_text())["client_email"]
    admin = bigquery.Client(project=settings.bq_project, location=settings.bq_location,
                            credentials=Credentials.from_service_account_file(args.admin_key, scopes=store.SCOPES))

    for name in args.datasets:
        ds = bigquery.Dataset(f"{settings.bq_project}.{name}")
        ds.location = settings.bq_location
        ds.description = DESCRIPTIONS.get(name, "Feed Health Monitor")
        ds = admin.create_dataset(ds, exists_ok=True)
        grant(admin, ds, "WRITER", collector_email)
        if args.dashboard_reader:  # the hosted app reads feed_health; local runs, with the same key, dev too
            grant(admin, ds, "READER", args.dashboard_reader)

        collector = store.client_for(replace(settings, bq_dataset=name))
        created = store.ensure_schema(collector, replace(settings, bq_dataset=name))
        print(f"{name}: tables and views ready as the collector: {', '.join(created)}")


if __name__ == "__main__":
    main()
