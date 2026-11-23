-- Fails if a date x package appears more than once (for example a load that appended instead of replacing).
SELECT
	download_date
	, package
	, COUNT(*) AS row_count
FROM {{ ref('stg_npm__downloads') }}
GROUP BY 1, 2
HAVING COUNT(*) > 1
