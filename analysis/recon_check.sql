-- Recon check: my PyPI counts vs pypistats `with_mirrors`, by date and package.
-- Both sides count every download row (mirrors and NULL installers included), so they should tie exactly.
WITH
	mine AS (
		SELECT
			download_date AS date
			, package
			, SUM(downloads) AS my_downloads
		FROM `ancient-voltage-258310.raw_pypi.downloads_agg`
		GROUP BY 1, 2
	)
	, theirs AS (
		SELECT
			date
			, package
			, downloads AS pypistats_downloads
		FROM `ancient-voltage-258310.raw_pypistats.overall`
		WHERE category = 'with_mirrors'
	)
SELECT
	COALESCE(m.date, t.date) AS date
	, COALESCE(m.package, t.package) AS package
	, m.my_downloads
	, t.pypistats_downloads
	, m.my_downloads - t.pypistats_downloads AS gap
	, ROUND(SAFE_DIVIDE(m.my_downloads - t.pypistats_downloads, t.pypistats_downloads) * 100, 3) AS gap_pct
	, CASE
		WHEN m.date IS NULL THEN 'missing_mine'
		WHEN t.date IS NULL THEN 'missing_pypistats'
		WHEN m.my_downloads = t.pypistats_downloads THEN 'tie'
		ELSE 'gap'
	END AS status
FROM mine AS m
FULL OUTER JOIN theirs AS t
	ON m.date = t.date
	AND m.package = t.package
ORDER BY 1, 2
