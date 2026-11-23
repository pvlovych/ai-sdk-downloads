-- Fails if the classification joins add or drop downloads on any date
-- (for example 2 vendor map rows valid on the same date).
WITH
	stg AS (
		SELECT
			download_date
			, SUM(downloads) AS downloads
			, COUNT(*) AS row_count
		FROM {{ ref('stg_pypi__downloads') }}
		GROUP BY 1
	)
	, classified AS (
		SELECT
			download_date
			, SUM(downloads) AS downloads
			, COUNT(*) AS row_count
		FROM {{ ref('int_pypi__downloads_classified') }}
		GROUP BY 1
	)
SELECT
	COALESCE(s.download_date, c.download_date) AS download_date
	, s.downloads AS stg_downloads
	, c.downloads AS classified_downloads
	, s.row_count AS stg_rows
	, c.row_count AS classified_rows
FROM stg AS s
FULL OUTER JOIN classified AS c
	ON s.download_date = c.download_date
WHERE s.download_date IS NULL
	OR c.download_date IS NULL
	OR s.downloads != c.downloads
	OR s.row_count != c.row_count
