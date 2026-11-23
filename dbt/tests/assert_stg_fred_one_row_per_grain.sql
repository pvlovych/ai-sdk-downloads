-- Fails if a series x month x vintage appears more than once, or a month has 2 current vintages.
SELECT
	series_id
	, observation_month
	, vintage_start
	, COUNT(*) AS row_count
FROM {{ ref('stg_fred__ramp_ai') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1
UNION ALL
SELECT
	series_id
	, observation_month
	, MAX(vintage_start)
	, COUNTIF(is_latest_vintage)
FROM {{ ref('stg_fred__ramp_ai') }}
GROUP BY 1, 2
HAVING COUNTIF(is_latest_vintage) != 1
