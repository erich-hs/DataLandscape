def pypi_aggregate_insert_query(
    target_table: str,
    reference_date: str
) -> str:
    return f"""INSERT INTO {target_table}
WITH downloads_last_7_days AS (
	SELECT
	    project,
	    project_version,
	    country_code,
		SUM(download_count) AS download_count
	FROM pypi_file_downloads
	WHERE download_date BETWEEN date_add('day', -6, DATE('{reference_date}')) AND DATE('{reference_date}')
	GROUP BY project, project_version, country_code
),
downloads_last_30_days AS (
	SELECT
	    project,
	    project_version,
	    country_code,
		SUM(download_count) AS download_count
	FROM pypi_file_downloads
	WHERE download_date BETWEEN date_add('day', -29, DATE('{reference_date}')) AND DATE('{reference_date}')
	GROUP BY project, project_version, country_code
)
SELECT
    DATE('{reference_date}') AS reference_date,
	l30.project,
	l30.project_version,
	l30.country_code,
	COALESCE(l7.download_count, 0) AS downloads_last_7_days,
	l30.download_count AS downloads_last_30_days
FROM downloads_last_30_days l30
	LEFT JOIN downloads_last_7_days l7
	    ON l30.project = l7.project
	    AND l30.project_version = l7.project_version
	    AND l30.country_code = l7.country_code
ORDER BY project, country_code, project_version
"""