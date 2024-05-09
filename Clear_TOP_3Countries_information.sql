SELECT -- table created to have results that I needed with all columns but just of TOP3 countries
    *
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
WHERE country IN ("India",'United States','Canada') -- Filtered countries that I needed
ORDER BY
    clear_table.user_pseudo_id DESC,
    clear_table.event_timestamp
