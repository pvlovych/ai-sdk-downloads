-- Late-row check: count 1 day in the public PyPI table now and compare it with the pypistats snapshot.
-- Same filter as ingest/pypi.py: whole UTC day, the 6 mapped PyPI packages, every row counted.
-- Run it on 2 dates. If my count is above pypistats and grows between runs, rows arrive late.
-- About 1.6 GiB a run. Change the date in both CTEs.
WITH
	public_now AS (
		SELECT
			file.project AS package
			, COUNT(*) AS public_downloads
		FROM `bigquery-public-data.pypi.file_downloads`
		WHERE timestamp >= TIMESTAMP('2026-11-21')
			AND timestamp < TIMESTAMP('2026-11-22')
			AND file.project IN ('anthropic', 'google-genai', 'google-generativeai', 'openai', 'stripe', 'twilio')
		GROUP BY 1
	)
	, pypistats AS (
		SELECT
			package
			, downloads AS pypistats_downloads
			, loaded_at AS pypistats_loaded_at
		FROM `ancient-voltage-258310.raw_pypistats.overall`
		WHERE category = 'with_mirrors'
			AND date = '2026-11-21'
		QUALIFY ROW_NUMBER() OVER (PARTITION BY package ORDER BY loaded_at DESC) = 1
	)
SELECT
	CURRENT_TIMESTAMP() AS counted_at
	, COALESCE(p.package, s.package) AS package
	, p.public_downloads
	, s.pypistats_downloads
	, p.public_downloads - s.pypistats_downloads AS gap
	, ROUND(SAFE_DIVIDE(p.public_downloads - s.pypistats_downloads, s.pypistats_downloads) * 100, 3) AS gap_pct
	, s.pypistats_loaded_at
FROM public_now AS p
FULL OUTER JOIN pypistats AS s
	ON p.package = s.package
ORDER BY 2
