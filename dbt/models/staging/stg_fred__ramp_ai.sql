-- 1 row per series x month x vintage. FRED sends realtime_start only when a value
-- changes, so a vintage is current until the day before the next one for the same month.
WITH
	source AS (
		SELECT *
		FROM {{ source('raw_fred', 'ramp_ai') }}
	)
SELECT
	series_id
	, observation_date AS observation_month
	, realtime_start AS vintage_start
	, DATE_SUB(
		LEAD(realtime_start) OVER (PARTITION BY series_id, observation_date ORDER BY realtime_start)
		, INTERVAL 1 DAY
	) AS vintage_end
	, LEAD(realtime_start) OVER (PARTITION BY series_id, observation_date ORDER BY realtime_start) IS NULL AS is_latest_vintage
	, SAFE_CAST(NULLIF(value, '.') AS FLOAT64) AS adoption_pct
	, loaded_at
FROM source
