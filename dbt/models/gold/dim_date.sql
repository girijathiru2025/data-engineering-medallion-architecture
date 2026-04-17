{{
    config(
        materialized='table'
    )
}}

/*
  Gold Layer: dim_date
  ----------------------
  Date dimension covering 2015-01-01 through 2030-12-31.
  Generated using a recursive CTE — no external seed file needed.
  Loaded once; re-run is idempotent (materialized='table').
*/

WITH date_series AS (
    SELECT CAST('2015-01-01' AS DATE) AS full_date
    UNION ALL
    SELECT (full_date + INTERVAL '1 day')::DATE
    FROM date_series
    WHERE full_date < '2030-12-31'
)

SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::INT           AS date_key,
    full_date,
    EXTRACT(YEAR  FROM full_date)::SMALLINT        AS year,
    EXTRACT(QUARTER FROM full_date)::SMALLINT      AS quarter,
    EXTRACT(MONTH FROM full_date)::SMALLINT        AS month,
    TO_CHAR(full_date, 'Month')                    AS month_name,
    EXTRACT(WEEK  FROM full_date)::SMALLINT        AS week_of_year,
    EXTRACT(DOW   FROM full_date)::SMALLINT        AS day_of_week,
    TO_CHAR(full_date, 'Day')                      AS day_name,
    EXTRACT(DOW   FROM full_date) IN (0, 6)        AS is_weekend,
    full_date = DATE_TRUNC('month', full_date)
               + INTERVAL '1 month'
               - INTERVAL '1 day'                  AS is_month_end,
    full_date = DATE_TRUNC('quarter', full_date)
               + INTERVAL '3 months'
               - INTERVAL '1 day'                  AS is_quarter_end
FROM date_series
