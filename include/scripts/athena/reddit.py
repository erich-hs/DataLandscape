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
    created_date,
    id,
    unn.polarities_struct.project AS project,
    unn.polarities_struct.polarity AS polarity
FROM reddit_projects_mentions m
CROSS JOIN UNNEST(m.projects_mentions_polarity) AS unn (polarities_struct)
WHERE projects_mentions_polarity IS NOT NULL AND created_date = DATE('{reference_date}')
)
SELECT
    m.created_date,
    m.project,
    SUM(m.mentions_count) AS mentions_count,
    AVG(p.polarity) AS avg_polarity
FROM mentions m
    JOIN polarities p ON m.id = p.id AND m.project = p.project
GROUP BY m.created_date, m.project
ORDER BY created_date DESC, mentions_count DESC, avg_polarity DESC
"""

def reddit_cumulative_aggregate_insert_query(
    target_table: str,
    reference_date: str
) -> str:
    return f"""INSERT INTO {target_table}
WITH mentions_today AS (
    SELECT
        project,
        mentions_count,
        avg_polarity
    FROM agg_reddit_daily_mentions_polarity
    WHERE created_date = DATE('{reference_date}')
),
mentions_last_7_days AS (
    SELECT
        project,
        SUM(mentions_count) AS mentions_count,
        AVG(avg_polarity) AS avg_polarity
    FROM agg_reddit_daily_mentions_polarity
    WHERE created_date BETWEEN date_add('day', -6, DATE('{reference_date}')) AND DATE('{reference_date}')
    GROUP BY project
),
mentions_last_30_days AS (
    SELECT
        project,
        SUM(mentions_count) AS mentions_count,
        AVG(avg_polarity) AS avg_polarity
    FROM agg_reddit_daily_mentions_polarity
    WHERE created_date BETWEEN date_add('day', -29, DATE('{reference_date}')) AND DATE('{reference_date}')
    GROUP BY project
),
mentions_last_90_days AS (
    SELECT
        project,
        SUM(mentions_count) AS mentions_count,
        AVG(avg_polarity) AS avg_polarity
    FROM agg_reddit_daily_mentions_polarity
    WHERE created_date BETWEEN date_add('day', -89, DATE('{reference_date}')) AND DATE('{reference_date}')
    GROUP BY project
)
SELECT
    DATE('{reference_date}') AS reference_date,
    COALESCE(COALESCE(COALESCE(t.project, l7.project), l30.project), l90.project) AS project,
    COALESCE(t.mentions_count, 0) AS mentions_count,
    COALESCE(t.avg_polarity, 0) AS avg_polarity,
    COALESCE(l7.mentions_count, 0) AS mentions_count_last_7_days,
    COALESCE(l7.avg_polarity, 0) AS avg_polarity_last_7_days,
    COALESCE(l30.mentions_count, 0) AS mentions_count_last_30_days,
    COALESCE(l30.avg_polarity, 0) AS avg_polarity_last_30_days,
    l90.mentions_count AS mentions_count_last_90_days,
    l90.avg_polarity AS avg_polarity_last_90_days
FROM mentions_today t
    RIGHT JOIN mentions_last_7_days l7 ON t.project = l7.project
    RIGHT JOIN mentions_last_30_days l30 ON l7.project = l30.project
    RIGHT JOIN mentions_last_90_days l90 ON l30.project = l90.project
"""