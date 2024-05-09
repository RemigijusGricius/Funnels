WITH summary AS (
    SELECT
        clear_table.event_name AS event_name,
        clear_table.country AS country,
        COUNT(clear_table.user_pseudo_id) AS event_count
    FROM
        `turing_data_analytics.raw_events` AS clear_table
    JOIN (
        SELECT
            user_pseudo_id,
            event_name,
            MIN(event_timestamp) AS earliest_timestamp
        FROM
            `turing_data_analytics.raw_events`
        GROUP BY
            user_pseudo_id, event_name
    ) AS earliest_events
    ON
        clear_table.user_pseudo_id = earliest_events.user_pseudo_id
        AND clear_table.event_name = earliest_events.event_name
        AND clear_table.event_timestamp = earliest_events.earliest_timestamp
    WHERE
        clear_table.country IN ('India', 'United States', 'Canada')
        AND clear_table.event_name IN ('first_visit','scroll','view_item','add_to_cart','add_payment_info','purchase')
    GROUP BY
        clear_table.event_name,
        clear_table.country
    ORDER BY
        clear_table.country DESC,
        COUNT(*) DESC
)--Summary CTE and left just colums that needed to calculate funnel actions

SELECT
    CASE -- to have column with event seq.
        WHEN summary.event_name = 'first_visit' THEN 1
        WHEN summary.event_name = 'scroll' THEN 2
        WHEN summary.event_name = 'view_item' THEN 3
        WHEN summary.event_name = 'add_to_cart' THEN 4
        WHEN summary.event_name = 'add_payment_info' THEN 5
        WHEN summary.event_name = 'purchase' THEN 6
    END AS event_order,
    summary.event_name,--Column with events name
    SUM(CASE WHEN summary.country = 'United States' THEN summary.event_count ELSE 0 END) AS United_States_events_count, -- events for country
    SUM(CASE WHEN summary.country = 'India' THEN summary.event_count ELSE 0 END) AS India_events_count,
    SUM(CASE WHEN summary.country = 'Canada' THEN summary.event_count ELSE 0 END) AS Canada_events_count,
    ROUND(SUM(CASE WHEN summary.event_name = 'first_visit' THEN summary.event_count
             WHEN summary.event_name = 'scroll' THEN summary.event_count
             WHEN summary.event_name = 'view_item' THEN summary.event_count
             WHEN summary.event_name = 'add_to_cart' THEN summary.event_count
             WHEN summary.event_name = 'add_payment_info' THEN summary.event_count
             WHEN summary.event_name = 'purchase' THEN summary.event_count ELSE 0
    END)/(SELECT SUM(event_count) FROM summary WHERE event_name = 'first_visit')*100,2)
        AS Full_perc,--Calculated sum of total event in all three countries and divided from start qty that was event_1 total sum
     ROUND(SUM(CASE WHEN summary.event_name = 'first_visit' AND country = 'United States' THEN summary.event_count
             WHEN summary.event_name = 'scroll' AND country = 'United States' THEN summary.event_count
             WHEN summary.event_name = 'view_item' AND country = 'United States' THEN summary.event_count
             WHEN summary.event_name = 'add_to_cart' AND country = 'United States' THEN summary.event_count
             WHEN summary.event_name = 'add_payment_info' AND country = 'United States' THEN summary.event_count
             WHEN summary.event_name = 'purchase' AND country = 'United States' THEN summary.event_count ELSE 0
    END)/(SELECT SUM(event_count) FROM summary WHERE event_name = 'first_visit' AND country = 'United States') * 100, 2) AS United_states_perc_drop,--Done same logic as total group just additional filter added to country to filter out what country we are analyzing
    ROUND(SUM(CASE WHEN summary.event_name = 'first_visit' AND country = 'India' THEN summary.event_count
             WHEN summary.event_name = 'scroll' AND country = 'India' THEN summary.event_count
             WHEN summary.event_name = 'view_item' AND country = 'India' THEN summary.event_count
             WHEN summary.event_name = 'add_to_cart' AND country = 'India' THEN summary.event_count
             WHEN summary.event_name = 'add_payment_info' AND country = 'India' THEN summary.event_count
             WHEN summary.event_name = 'purchase' AND country = 'India' THEN summary.event_count ELSE 0
    END)/(SELECT SUM(event_count) FROM summary WHERE event_name = 'first_visit' AND country = 'India') * 100, 2) AS India_states_perc_drop,
    ROUND(SUM(CASE WHEN summary.event_name = 'first_visit' AND country = 'Canada' THEN summary.event_count
             WHEN summary.event_name = 'scroll' AND country = 'Canada' THEN summary.event_count
             WHEN summary.event_name = 'view_item' AND country = 'Canada' THEN summary.event_count
             WHEN summary.event_name = 'add_to_cart' AND country = 'Canada' THEN summary.event_count
             WHEN summary.event_name = 'add_payment_info' AND country = 'Canada' THEN summary.event_count
             WHEN summary.event_name = 'purchase' AND country = 'Canada' THEN summary.event_count ELSE 0
    END)/(SELECT SUM(event_count) FROM summary WHERE event_name = 'first_visit' AND country = 'Canada') * 100, 2) AS Canada_states_perc_drop
FROM
    summary
GROUP BY
    event_order,
    summary.event_name
ORDER BY
    event_order;
