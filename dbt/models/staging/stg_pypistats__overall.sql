-- date x package x category. Keeps the latest pull for each date, because every pull is appended.
WITH
	source AS (
		SELECT *
		FROM {{ source('raw_pypistats', 'overall') }}
	)
SELECT
	date AS download_date
	, REGEXP_REPLACE(LOWER(package), r'[-_.]+', '-') AS package
	, category
	, downloads
	, loaded_at
FROM source
QUALIFY ROW_NUMBER() OVER (PARTITION BY date, package, category ORDER BY loaded_at DESC) = 1
