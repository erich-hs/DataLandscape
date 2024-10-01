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