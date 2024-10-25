def reddit_daily_aggregate_insert_query(
    target_table: str,
    reference_date: str
) -> str:
    return f"""INSERT INTO {target_table}
WITH mentions AS (
    SELECT
        created_date,
        id,
        content_type,
        unn.mentions_struct.project AS project,
        unn.mentions_struct.mentions AS mentions_count
    FROM reddit_projects_mentions m
    CROSS JOIN UNNEST(m.projects_mentions) AS unn (mentions_struct)
    WHERE projects_mentions IS NOT NULL AND created_date = DATE('{reference_date}')
),
polarities AS (
    SELECT
        m.created_date,
        m.id,
        unn.polarities_struct.project AS project,
        unn.polarities_struct.polarity AS polarity,
        p.polarity_category
    FROM reddit_projects_mentions m
    CROSS JOIN UNNEST(m.projects_mentions_polarity) AS unn (polarities_struct)
    LEFT JOIN dim_polarity_categories p ON unn.polarities_struct.polarity BETWEEN p.polarity_lower_bound AND p.polarity_upper_bound
    WHERE projects_mentions_polarity IS NOT NULL AND created_date = DATE('{reference_date}')
)
SELECT
    m.created_date,
    m.project,
    p.polarity_category,
    SUM(m.mentions_count) AS mentions_count,
    AVG(p.polarity) AS avg_polarity
FROM mentions m
JOIN polarities p ON m.id = p.id AND m.project = p.project
GROUP BY m.created_date, m.project, p.polarity_category
"""

def reddit_cumulative_aggregate_insert_query(
    target_table: str,
    reference_date: str
) -> str:
    return f"""INSERT INTO {target_table}
WITH mentions_today AS (
    SELECT
        project,
        polarity_category,
        mentions_count,
        avg_polarity
    FROM agg_reddit_daily_mentions_polarity
    WHERE created_date = DATE('{reference_date}')
),
mentions_last_7_days AS (
    SELECT
        project,
        polarity_category,
        SUM(mentions_count) AS mentions_count,
        AVG(avg_polarity) AS avg_polarity
    FROM agg_reddit_daily_mentions_polarity
    WHERE created_date BETWEEN date_add('day', -6, DATE('{reference_date}')) AND DATE('{reference_date}')
    GROUP BY project, polarity_category
),
mentions_last_30_days AS (
    SELECT
        project,
        polarity_category,
        SUM(mentions_count) AS mentions_count,
        AVG(avg_polarity) AS avg_polarity
    FROM agg_reddit_daily_mentions_polarity
    WHERE created_date BETWEEN date_add('day', -29, DATE('{reference_date}')) AND DATE('{reference_date}')
    GROUP BY project, polarity_category
),
mentions_last_90_days AS (
    SELECT
        project,
        polarity_category,
        SUM(mentions_count) AS mentions_count,
        AVG(avg_polarity) AS avg_polarity
    FROM agg_reddit_daily_mentions_polarity
    WHERE created_date BETWEEN date_add('day', -89, DATE('{reference_date}')) AND DATE('{reference_date}')
    GROUP BY project, polarity_category
)
SELECT
    DATE('{reference_date}') AS reference_date,
    p.project_name AS project,
    c.polarity_category,
    COALESCE(t.mentions_count, 0) AS mentions_count,
    COALESCE(t.avg_polarity, 0) AS avg_polarity,
    COALESCE(l7.mentions_count, 0) AS mentions_count_last_7_days,
    COALESCE(l7.avg_polarity, 0) AS avg_polarity_last_7_days,
    COALESCE(l30.mentions_count, 0) AS mentions_count_last_30_days,
    COALESCE(l30.avg_polarity, 0) AS avg_polarity_last_30_days,
    COALESCE(l90.mentions_count, 0) AS mentions_count_last_90_days,
    COALESCE(l90.avg_polarity, 0) AS avg_polarity_last_90_days
FROM dim_tracked_projects p
CROSS JOIN dim_polarity_categories c
LEFT JOIN mentions_today t ON p.project_name = t.project AND c.polarity_category = t.polarity_category
LEFT JOIN mentions_last_7_days l7 ON p.project_name = l7.project AND c.polarity_category = l7.polarity_category
LEFT JOIN mentions_last_30_days l30 ON p.project_name = l30.project AND c.polarity_category = l30.polarity_category
LEFT JOIN mentions_last_90_days l90 ON p.project_name = l90.project AND c.polarity_category = l90.polarity_category
"""