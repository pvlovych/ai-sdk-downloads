"""Copy npm daily downloads for the mapped packages into raw_npm.downloads_daily.

1 call per package and window, because scoped packages don't work in bulk calls.
The range endpoint serves at most 550 days per call and moves the start date
without an error when the range is longer, so the script asks for 365-day
windows and stops if npm moves the start.

npm returns 0 for days it hasn't counted yet (today, sometimes yesterday). The
script drops the zero days after the last non-zero day, so a missing day stays
missing instead of becoming 0.

Idempotent: delete the package-days that the load table has, then insert, in 1
transaction.

    python -m ingest.npm                                    # last 7 days, today included
    python -m ingest.npm --start 2025-05-23 --end 2026-11-23
"""
import datetime as dt
import time

import requests
from google.cloud import bigquery

from ingest.common import GIB, PROJECT, date_args, get_client, log, packages

TABLE = f"{PROJECT}.raw_npm.downloads_daily"
LOAD_TABLE = f"{PROJECT}.raw_npm._load_downloads_daily"
URL = "https://api.npmjs.org/downloads/range/{start}:{end}/{package}"
HEADERS = {"User-Agent": "ai-sdk-downloads-ingest"}
WINDOW_DAYS = 365
PAUSE_S = 1
RETRIES = 3

SCHEMA = [
    bigquery.SchemaField("download_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("package", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("downloads", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

SCRIPT = f"""
BEGIN TRANSACTION;
DELETE FROM `{TABLE}` AS t
WHERE EXISTS (
  SELECT 1
  FROM `{LOAD_TABLE}` AS l
  WHERE l.package = t.package
    AND l.download_date = t.download_date
);
INSERT INTO `{TABLE}` SELECT * FROM `{LOAD_TABLE}`;
COMMIT TRANSACTION;
"""


def windows(start: dt.date, end: dt.date):
    while start <= end:
        stop = min(start + dt.timedelta(days=WINDOW_DAYS - 1), end)
        yield start, stop
        start = stop + dt.timedelta(days=1)


def fetch(package: str, start: dt.date, end: dt.date) -> list[dict]:
    url = URL.format(start=start, end=end, package=package)
    for attempt in range(RETRIES + 1):
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 429 and attempt < RETRIES:
            wait = 30 * (attempt + 1)
            log(step="rate_limited", package=package, wait_s=wait)
            time.sleep(wait)
            continue
        r.raise_for_status()
        body = r.json()
        if body["start"] != start.isoformat():
            raise SystemExit(f"npm moved the start for {package}: asked {start}, got {body['start']}")
        return body["downloads"]
    raise RuntimeError("unreachable")


def drop_uncounted_tail(days: list[dict]) -> list[dict]:
    last = max((i for i, d in enumerate(days) if d["downloads"] > 0), default=-1)
    return days[: last + 1]


def main() -> None:
    args = date_args(default_days=7, description=__doc__)
    client = get_client()
    table = bigquery.Table(TABLE, schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(field="download_date")
    table.clustering_fields = ["package"]
    client.create_table(table, exists_ok=True)

    loaded_at = dt.datetime.now(dt.timezone.utc).isoformat()
    rows = []
    calls = 0
    for pkg in packages("npm"):
        days = []
        for start, end in windows(args.start, args.end):
            if calls:
                time.sleep(PAUSE_S)
            days += fetch(pkg, start, end)
            calls += 1
        kept = drop_uncounted_tail(days)
        rows += [{"download_date": d["day"], "package": pkg, "downloads": d["downloads"],
                  "loaded_at": loaded_at} for d in kept]
        log(step="fetched", package=pkg, days=len(days), kept=len(kept),
            last_day=kept[-1]["day"] if kept else None)

    if not rows:
        raise SystemExit("npm returned no counted days")

    client.load_table_from_json(
        rows, LOAD_TABLE,
        job_config=bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_TRUNCATE"),
    ).result()
    client.query(SCRIPT, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=GIB)).result()
    log(step="loaded", rows=len(rows), calls=calls, start=args.start, end=args.end, loaded_at=loaded_at)


if __name__ == "__main__":
    main()
