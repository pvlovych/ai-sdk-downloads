"""Shared helpers: BigQuery client, dates, package list, byte guard."""
import argparse
import csv
import datetime as dt
import hashlib
import os
import subprocess
from pathlib import Path

from google.cloud import bigquery

PROJECT = os.environ.get("BQ_PROJECT", "ancient-voltage-258310")
# sha256 of the personal Google account, so the address stays out of a public repo.
PERSONAL_ACCOUNT_SHA256 = "ec7f1e1555707e6651257605aa1fa4763bb1d1cfef554d44909b4d9d3e99aa5c"
SEED = Path(__file__).resolve().parent.parent / "dbt" / "seeds" / "vendor_package_map.csv"
GIB = 1024**3


def get_client() -> bigquery.Client:
    """CI uses Workload Identity Federation (ADC). Locally, use the `personal`
    gcloud config and refuse anything else. The machine-wide ADC is the work
    account, so it must never be picked up here."""
    if os.environ.get("GITHUB_ACTIONS") == "true":
        return bigquery.Client(project=PROJECT, location="US")

    from google.oauth2.credentials import Credentials

    env = {**os.environ, "CLOUDSDK_ACTIVE_CONFIG_NAME": "personal"}
    account = subprocess.run(
        ["gcloud", "config", "get-value", "account"],
        env=env, capture_output=True, text=True, check=True,
    ).stdout.strip()
    if hashlib.sha256(account.encode()).hexdigest() != PERSONAL_ACCOUNT_SHA256:
        raise SystemExit(f"gcloud config 'personal' is {account!r}, not the personal account")
    token = subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        env=env, capture_output=True, text=True, check=True,
    ).stdout.strip()
    return bigquery.Client(project=PROJECT, credentials=Credentials(token), location="US")


def secret(name: str) -> str:
    """Environment first (GitHub Actions secrets), then the git-ignored .env file."""
    if os.environ.get(name):
        return os.environ[name]
    env_file = Path(__file__).resolve().parent.parent / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            key, sep, value = line.partition("=")
            if sep and key.strip() == name:
                return value.strip()
    raise SystemExit(f"{name} is not set (environment or .env)")


def packages(ecosystem: str) -> list[str]:
    with SEED.open() as f:
        return sorted({r["package"] for r in csv.DictReader(f) if r["ecosystem"] == ecosystem})


def date_args(default_days: int, description: str) -> argparse.Namespace:
    """--start / --end in UTC. Default: the last `default_days` days, today included."""
    today = dt.datetime.now(dt.timezone.utc).date()
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--start", type=dt.date.fromisoformat, default=today - dt.timedelta(days=default_days - 1))
    p.add_argument("--end", type=dt.date.fromisoformat, default=today)
    p.add_argument("--max-gib", type=float, default=10.0, help="byte cap for the job")
    args = p.parse_args()
    if args.start > args.end:
        raise SystemExit("--start is after --end")
    return args


def check_bytes(client: bigquery.Client, sql: str, params: list, max_bytes: int) -> int:
    """Dry run first. Stop before a scan that exceeds the cap."""
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False, query_parameters=params),
    )
    scanned = job.total_bytes_processed
    if scanned > max_bytes:
        raise SystemExit(f"dry run: {scanned / GIB:.1f} GiB is over the cap of {max_bytes / GIB:.1f} GiB")
    return scanned


def log(**fields) -> None:
    print(" ".join(f"{k}={v}" for k, v in fields.items()), flush=True)
