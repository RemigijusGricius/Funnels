SELECT-- Selected everything that in final results I would have all columns
    *
FROM
    `turing_data_analytics.raw_events` AS clear_table
JOIN (
    SELECT
        user_pseudo_id,
        event_name,
        MIN(event_timestamp) AS earliest_timestamp -- subquery to have ID, event name and earliest event time to remove event if user did same action more than once
    FROM
        `turing_data_analytics.raw_events`
    GROUP BY
        user_pseudo_id, event_name
) AS earliest_events
ON
    clear_table.user_pseudo_id = earliest_events.user_pseudo_id
    AND clear_table.event_name = earliest_events.event_name
    AND clear_table.event_timestamp = earliest_events.earliest_timestamp -- connected by ID event name and event time should be equal to earliest event.
ORDER BY
    clear_table.user_pseudo_id DESC,
    clear_table.event_timestamp
