-- Fails if a date x package x category appears more than once after the latest-pull filter.
SELECT
	download_date
	, package
	, category
	, COUNT(*) AS row_count
FROM {{ ref('stg_pypistats__overall') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
