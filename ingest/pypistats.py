"""Append the pypistats.org /overall response to raw_pypistats.overall.

1 call per package returns about 180 days for both categories (with_mirrors,
without_mirrors). Every pull is appended with the same loaded_at. Staging picks
the latest pull for each date. This is the independent number for the recon.

    python -m ingest.pypistats
"""
import datetime as dt
import time

import requests
from google.cloud import bigquery

from ingest.common import PROJECT, get_client, log, packages

TABLE = f"{PROJECT}.raw_pypistats.overall"
URL = "https://pypistats.org/api/packages/{package}/overall"
HEADERS = {"User-Agent": "ai-sdk-downloads-ingest"}
PAUSE_S = 3
RETRIES = 3

SCHEMA = [
    bigquery.SchemaField("package", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("category", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("downloads", "INT64", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]


def fetch(package: str) -> list[dict]:
    for attempt in range(RETRIES + 1):
        r = requests.get(URL.format(package=package), headers=HEADERS, timeout=30)
        if r.status_code == 429 and attempt < RETRIES:
            wait = 30 * (attempt + 1)
            log(step="rate_limited", package=package, wait_s=wait)
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()["data"]
    raise RuntimeError("unreachable")


def main() -> None:
    client = get_client()
    table = bigquery.Table(TABLE, schema=SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(field="date")
    table.clustering_fields = ["package"]
    client.create_table(table, exists_ok=True)

    loaded_at = dt.datetime.now(dt.timezone.utc).isoformat()
    rows = []
    for i, pkg in enumerate(packages("pypi")):
        if i:
            time.sleep(PAUSE_S)
        data = fetch(pkg)
        rows += [{"package": pkg, "category": d["category"], "date": d["date"],
                  "downloads": d["downloads"], "loaded_at": loaded_at} for d in data]
        log(step="fetched", package=pkg, rows=len(data))

    job = client.load_table_from_json(
        rows, TABLE,
        job_config=bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_APPEND"),
    )
    job.result()
    log(step="loaded", rows=len(rows), loaded_at=loaded_at)


if __name__ == "__main__":
    main()
