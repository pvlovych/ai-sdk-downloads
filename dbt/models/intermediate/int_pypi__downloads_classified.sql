-- Same grain as stg_pypi__downloads. Adds the vendor, the mirror flag, the CI flag and the partial-day flag.
-- No rows are removed. The marts decide what "clean" means.
WITH
	downloads AS (
		SELECT *
		FROM {{ ref('stg_pypi__downloads') }}
	)
	, vendor_map AS (
		SELECT
			REGEXP_REPLACE(LOWER(package), r'[-_.]+', '-') AS package
			, vendor
			, is_control
			, valid_from
			, valid_to
		FROM {{ ref('vendor_package_map') }}
		WHERE ecosystem = 'pypi'
	)
	, mirrors AS (
		SELECT installer
		FROM {{ ref('pypi_mirror_installers') }}
	)
SELECT
	d.download_date
	, d.package
	, v.vendor
	, v.is_control
	, d.version
	, d.installer
	, d.country_code
	, d.python_minor
	-- Same rule as pypistats: exact, case-sensitive installer name.
	, m.installer IS NOT NULL AS is_mirror
	-- pypistats without_mirrors drops these too (NULL NOT IN ... is not TRUE).
	, d.installer IS NULL AS is_unknown_installer
	-- The client sends TRUE or nothing. No row in raw has FALSE, so NULL means not reported as CI.
	, COALESCE(d.is_ci, FALSE) AS is_ci
	-- The load ran before the UTC day closed, so the day is incomplete.
	, d.loaded_at < TIMESTAMP(DATE_ADD(d.download_date, INTERVAL 1 DAY)) AS is_partial_day
	, d.downloads
	, d.loaded_at
FROM downloads AS d
LEFT JOIN vendor_map AS v
	ON d.package = v.package
	AND d.download_date >= COALESCE(v.valid_from, DATE '0001-01-01')
	AND d.download_date <= COALESCE(v.valid_to, DATE '9999-12-31')
LEFT JOIN mirrors AS m
	ON d.installer = m.installer
