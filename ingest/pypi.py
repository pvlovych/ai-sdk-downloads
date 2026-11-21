"""Copy PyPI downloads for the mapped packages into raw_pypi.downloads_agg.

Aggregated at ingest to date x package x version x installer x country x
Python minor x CI flag. No business logic: installer names stay as they are.

Idempotent: delete the date range, then insert, in 1 transaction. MERGE is not
used because installer, country and ci can be NULL, and MERGE does not match
NULL to NULL.

2 steps. Step 1 is a plain query from the public table into a small load table.
Its byte estimate uses the clustering on `project` (1.6 GiB for 1 day). Inside a
DML script, BigQuery estimates the same SELECT without that pruning (271 GiB for
1 day), and the byte cap rejects it. Step 2 swaps the date range from the load
table, which is small.

    python -m ingest.pypi                                  # last 3 days, today included
    python -m ingest.pypi --start 2026-05-25 --end 2026-06-23 --max-gib 60
"""
from google.cloud import bigquery

from ingest.common import GIB, PROJECT, check_bytes, date_args, get_client, log, packages

TABLE = f"`{PROJECT}.raw_pypi.downloads_agg`"
LOAD_TABLE = f"{PROJECT}.raw_pypi._load_downloads_agg"

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  download_date DATE NOT NULL,
  package STRING NOT NULL,
  version STRING,
  installer STRING,
  country STRING,
  python_minor STRING,
  ci BOOL,
  downloads INT64 NOT NULL,
  loaded_at TIMESTAMP NOT NULL
)
PARTITION BY download_date
CLUSTER BY package
"""

SELECT = """
SELECT
  DATE(timestamp) AS download_date,
  file.project AS package,
  file.version AS version,
  details.installer.name AS installer,
  country_code AS country,
  REGEXP_EXTRACT(details.python, r'^[0-9]+\\.[0-9]+') AS python_minor,
  details.ci AS ci,
  COUNT(*) AS downloads,
  CURRENT_TIMESTAMP() AS loaded_at
FROM `bigquery-public-data.pypi.file_downloads`
WHERE timestamp >= TIMESTAMP(@start)
  AND timestamp < TIMESTAMP(DATE_ADD(@end, INTERVAL 1 DAY))
  AND file.project IN UNNEST(@packages)
GROUP BY 1, 2, 3, 4, 5, 6, 7
"""

SCRIPT = f"""
BEGIN TRANSACTION;
DELETE FROM {TABLE} WHERE download_date BETWEEN @start AND @end;
INSERT INTO {TABLE} SELECT * FROM `{LOAD_TABLE}`;
COMMIT TRANSACTION;
"""


def main() -> None:
    args = date_args(default_days=3, description=__doc__)
    max_bytes = int(args.max_gib * GIB)
    client = get_client()
    pkgs = packages("pypi")
    params = [
        bigquery.ScalarQueryParameter("start", "DATE", args.start),
        bigquery.ScalarQueryParameter("end", "DATE", args.end),
        bigquery.ArrayQueryParameter("packages", "STRING", pkgs),
    ]

    client.query(DDL).result()
    scanned = check_bytes(client, SELECT, params, max_bytes)
    log(step="dry_run", start=args.start, end=args.end, packages=len(pkgs), gib=round(scanned / GIB, 2))

    job = client.query(
        SELECT,
        job_config=bigquery.QueryJobConfig(
            query_parameters=params,
            maximum_bytes_billed=max_bytes,
            destination=LOAD_TABLE,
            write_disposition="WRITE_TRUNCATE",
        ),
    )
    job.result()
    client.query(
        SCRIPT,
        job_config=bigquery.QueryJobConfig(query_parameters=params[:2], maximum_bytes_billed=GIB),
    ).result()
    rows = list(client.query(
        f"SELECT COUNT(*) AS n, SUM(downloads) AS d FROM {TABLE} WHERE download_date BETWEEN @start AND @end",
        job_config=bigquery.QueryJobConfig(query_parameters=params[:2]),
    ).result())[0]
    log(step="loaded", job=job.job_id, billed_gib=round((job.total_bytes_billed or 0) / GIB, 2),
        rows=rows.n, downloads=rows.d)


if __name__ == "__main__":
    main()
