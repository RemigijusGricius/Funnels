SELECT DISTINCT country, -- query to get top3 countries with the most overall events
COUNT(event_name) event_qty
FROM `turing_data_analytics.raw_events`
GROUP BY country
ORDER BY event_qty DESC
LIMIT 3
