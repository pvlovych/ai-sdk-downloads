-- 1 row per date x package. A date where every package is 0 is an npm gap,
-- not real usage (npm lost the day), so downloads become NULL there.
WITH
	source AS (
		SELECT *
		FROM {{ source('raw_npm', 'downloads_daily') }}
	)
	, flagged AS (
		SELECT
			download_date
			, package
			, downloads AS downloads_reported
			, MAX(downloads) OVER (PARTITION BY download_date) = 0 AS is_npm_gap
			, loaded_at
		FROM source
	)
SELECT
	download_date
	, package
	, IF(is_npm_gap, NULL, downloads_reported) AS downloads
	, downloads_reported
	, is_npm_gap
	, loaded_at
FROM flagged
