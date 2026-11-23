"""Copy every vintage of the 4 Ramp AI Index series from FRED into raw_fred.ramp_ai.

1 call per series returns the full real-time history (ALFRED). FRED returns a
row only when a value changes, with the period the value was current:
realtime_start to realtime_end. When a new vintage revises a value, FRED moves
realtime_end on the old row. So raw keeps realtime_start only and staging
derives the end from the next row.

Append only: rows whose series x observation date x realtime_start the raw
table doesn't have. A second run on the same day inserts 0 rows.

The value stays a string, as FRED sends it. FRED writes "." for a missing value.

The API key comes from FRED_API_KEY (GitHub Actions secret, or .env locally).

    python -m ingest.fred
"""
import datetime as dt
import time

import requests
from google.cloud import bigquery

from ingest.common import GIB, PROJECT, get_client, log, secret

SERIES = ["RAMPAIALL", "RAMPAIANTHROPIC", "RAMPAIOPENAI", "RAMPAIXAI"]
TABLE = f"{PROJECT}.raw_fred.ramp_ai"
LOAD_TABLE = f"{PROJECT}.raw_fred._load_ramp_ai"
URL = "https://api.stlouisfed.org/fred/series/observations"
HEADERS = {"User-Agent": "ai-sdk-downloads-ingest"}
PAUSE_S = 1
RETRIES = 3

SCHEMA = [
    bigquery.SchemaField("series_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("observation_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("realtime_start", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("value", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

SCRIPT = f"""
INSERT INTO `{TABLE}`
SELECT l.*
FROM `{LOAD_TABLE}` AS l
WHERE NOT EXISTS (
  SELECT 1
  FROM `{TABLE}` AS t
  WHERE t.series_id = l.series_id
    AND t.observation_date = l.observation_date
    AND t.realtime_start = l.realtime_start
)
"""


def fetch(series_id: str, api_key: str) -> list[dict]:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "realtime_start": "1776-07-04",  # FRED's earliest real-time date: all vintages
        "realtime_end": "9999-12-31",
    }
    for attempt in range(RETRIES + 1):
        try:
            r = requests.get(URL, params=params, headers=HEADERS, timeout=30)
        except requests.RequestException as e:
            # The exception text holds the full URL, key included.
            raise SystemExit(f"FRED request failed for {series_id}: {type(e).__name__}") from None
        if r.status_code == 429 and attempt < RETRIES:
            wait = 30 * (attempt + 1)
            log(step="rate_limited", series=series_id, wait_s=wait)
            time.sleep(wait)
            continue
        if r.status_code != 200:
            # The error body can echo the request URL, which holds the key.
            raise SystemExit(f"FRED returned {r.status_code} for {series_id}")
        body = r.json()
        if body["count"] > len(body["observations"]):
            raise SystemExit(f"FRED paged {series_id}: {body['count']} rows, got {len(body['observations'])}")
        return body["observations"]
    raise RuntimeError("unreachable")


def main() -> None:
    api_key = secret("FRED_API_KEY")
    client = get_client()
    table = bigquery.Table(TABLE, schema=SCHEMA)
    table.clustering_fields = ["series_id"]
    client.create_table(table, exists_ok=True)

    loaded_at = dt.datetime.now(dt.timezone.utc).isoformat()
    rows = []
    for i, series_id in enumerate(SERIES):
        if i:
            time.sleep(PAUSE_S)
        obs = fetch(series_id, api_key)
        rows += [{"series_id": series_id, "observation_date": o["date"],
                  "realtime_start": o["realtime_start"], "value": o["value"],
                  "loaded_at": loaded_at} for o in obs]
        log(step="fetched", series=series_id, rows=len(obs),
            vintages=len({o["realtime_start"] for o in obs}))

    client.load_table_from_json(
        rows, LOAD_TABLE,
        job_config=bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_TRUNCATE"),
    ).result()
    job = client.query(SCRIPT, job_config=bigquery.QueryJobConfig(maximum_bytes_billed=GIB))
    job.result()
    log(step="loaded", fetched=len(rows), inserted=job.num_dml_affected_rows, loaded_at=loaded_at)


if __name__ == "__main__":
    main()
