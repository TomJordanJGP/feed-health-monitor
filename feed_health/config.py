"""Settings: `config/feeds.yml` for feed membership and thresholds, the environment for
URLs and BigQuery. Locally the environment comes from `.env`; in CI, from repository secrets."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "feeds.yml"
ENV_PATH = PROJECT_ROOT / ".env"


@dataclass(frozen=True)
class FeedConfig:
    file: str
    label: str
    collect: bool
    show: bool
    imported: bool
    orange_hours: float
    red_hours: float
    configured: bool = True


@dataclass(frozen=True)
class MonitorConfig:
    expiry_grace_hours: float
    not_reaching_grace_hours: float
    export_max_shrink: float
    export_max_age_hours: float
    monitor_orange_hours: float
    monitor_red_hours: float


@dataclass(frozen=True)
class Settings:
    feeds: dict[str, FeedConfig]
    monitor: MonitorConfig
    default_orange_hours: float
    default_red_hours: float
    feed_map_url: str
    jobiqo_export_url: str
    bq_project: str
    bq_dataset: str
    bq_location: str
    credentials_path: Path | None

    def feed(self, file: str) -> FeedConfig:
        """Config for a feed file. A file on the feed map but missing from feeds.yml is checked
        like any imported feed (Jobiqo imports everything on the map); `configured=False` only
        adds a note in the status view so someone gives it a label."""
        if file in self.feeds:
            return self.feeds[file]
        return FeedConfig(file=file, label=file, collect=True, show=True, imported=True,
                          orange_hours=self.default_orange_hours, red_hours=self.default_red_hours,
                          configured=False)

    def table(self, name: str) -> str:
        return f"{self.bq_project}.{self.bq_dataset}.{name}"


def load_dotenv(path: Path = ENV_PATH) -> None:
    """Read KEY=VALUE lines into os.environ. Values already in the environment win, so CI
    secrets are never overridden by a stray local file."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def load_settings(config_path: Path = CONFIG_PATH, require_urls: bool = True) -> Settings:
    """The dashboard passes require_urls=False: it only reads BigQuery."""
    load_dotenv()
    raw = yaml.safe_load(config_path.read_text())
    defaults = raw.get("defaults") or {}
    orange = float(defaults.get("orange_hours", 5))
    red = float(defaults.get("red_hours", 10))

    feeds: dict[str, FeedConfig] = {}
    for file, entry in (raw.get("feeds") or {}).items():
        entry = entry or {}
        feeds[file] = FeedConfig(
            file=file,
            label=str(entry.get("label") or file),
            collect=bool(entry.get("collect", True)),
            show=bool(entry.get("show", True)),
            imported=bool(entry.get("imported", True)),
            orange_hours=float(entry.get("orange_hours", orange)),
            red_hours=float(entry.get("red_hours", red)),
        )

    m = raw.get("monitor") or {}
    monitor = MonitorConfig(
        expiry_grace_hours=float(m.get("expiry_grace_hours", 24)),
        not_reaching_grace_hours=float(m.get("not_reaching_grace_hours", 5)),
        export_max_shrink=float(m.get("export_max_shrink", 0.2)),
        export_max_age_hours=float(m.get("export_max_age_hours", 12)),
        monitor_orange_hours=float(m.get("monitor_orange_hours", 2)),
        monitor_red_hours=float(m.get("monitor_red_hours", 4)),
    )

    feed_map_url = os.environ.get("FEED_MAP_URL", "").strip()
    export_url = os.environ.get("JOBIQO_EXPORT_URL", "").strip()
    if require_urls and not (feed_map_url and export_url):
        raise RuntimeError("FEED_MAP_URL and JOBIQO_EXPORT_URL must be set (.env locally, secrets in CI)")

    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    credentials_path = None
    if creds:
        credentials_path = Path(creds)
        if not credentials_path.is_absolute():
            credentials_path = PROJECT_ROOT / credentials_path

    return Settings(
        feeds=feeds, monitor=monitor, default_orange_hours=orange, default_red_hours=red,
        feed_map_url=feed_map_url, jobiqo_export_url=export_url,
        bq_project=os.environ.get("BQ_PROJECT", "site-monitoring-421401"),
        bq_dataset=os.environ.get("BQ_DATASET", "feed_health_dev"),
        bq_location=os.environ.get("BQ_LOCATION", "EU"),
        credentials_path=credentials_path,
    )
