def pypi_cumulative_aggregate_insert_query(
    target_table: str,
    reference_date: str
) -> str:
    return f"""INSERT INTO {target_table}
WITH downloads_today AS (
    SELECT
        project,
        project_version,
        country_code,
        SUM(download_count) AS download_count
    FROM pypi_file_downloads
	WHERE download_date = DATE('{reference_date}')
	GROUP BY project, project_version, country_code
),
downloads_last_7_days AS (
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
),
downloads_last_90_days AS (
	SELECT
	    project,
	    project_version,
	    country_code,
		SUM(download_count) AS download_count
	FROM pypi_file_downloads
	WHERE download_date BETWEEN date_add('day', -89, DATE('{reference_date}')) AND DATE('{reference_date}')
	GROUP BY project, project_version, country_code
)
SELECT
    DATE('{reference_date}') AS reference_date,
    COALESCE(COALESCE(COALESCE(t.project, l7.project), l30.project), l90.project) AS project,
    COALESCE(COALESCE(COALESCE(t.project_version, l7.project_version), l30.project_version), l90.project_version) AS project_version,
    COALESCE(COALESCE(COALESCE(t.country_code, l7.country_code), l30.country_code), l90.country_code) AS country_code,
    COALESCE(t.download_count, 0) AS download_count,
    l7.download_count AS download_count_last_7_days,
    l30.download_count AS download_count_last_30_days,
    l90.download_count AS download_count_last_90_days
FROM downloads_today t
    RIGHT JOIN downloads_last_7_days l7 ON t.project = l7.project AND t.project_version = l7.project_version AND t.country_code = l7.country_code
    RIGHT JOIN downloads_last_30_days l30 ON l7.project = l30.project AND l7.project_version = l30.project_version AND l7.country_code = l30.country_code
    RIGHT JOIN downloads_last_90_days l90 ON l30.project = l90.project AND l30.project_version = l90.project_version AND l30.country_code = l90.country_code
"""