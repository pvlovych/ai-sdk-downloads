-- 1 row per raw aggregate row. Renames, casts and PEP 503 package names. No filters.
WITH
	source AS (
		SELECT *
		FROM {{ source('raw_pypi', 'downloads_agg') }}
	)
SELECT
	download_date
	, REGEXP_REPLACE(LOWER(package), r'[-_.]+', '-') AS package
	, version
	, installer
	, country AS country_code
	, python_minor
	, ci AS is_ci
	, downloads
	, loaded_at
FROM source
