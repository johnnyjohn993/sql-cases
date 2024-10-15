duplicate_date_driver AS (
    SELECT *,
        CASE WHEN COUNT(*) OVER (PARTITION BY status_date, driver_id) > 1 THEN 1 ELSE 0 END AS is_duplicate
    FROM
        initial_date_driver

