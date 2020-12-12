{{
  config(
    materialized = "table"
  )
}}


WITH date_spine AS (

  select DATE('2020-01-01') AS date_day UNION ALL
  select DATE('2020-01-02') AS date_day UNION ALL
  select DATE('2020-01-03') AS date_day UNION ALL
  select DATE('2020-01-04') AS date_day UNION ALL
  select DATE('2020-01-05') AS date_day UNION ALL
  select DATE('2020-01-06') AS date_day UNION ALL
  select DATE('2020-01-07') AS date_day UNION ALL
  select DATE('2020-01-08') AS date_day UNION ALL
  select DATE('2020-01-09') AS date_day UNION ALL
  select DATE('2020-01-10') AS date_day UNION ALL
  select DATE('2020-01-11') AS date_day UNION ALL
  select DATE('2020-01-12') AS date_day UNION ALL
  select DATE('2020-01-13') AS date_day UNION ALL
  select DATE('2020-01-14') AS date_day UNION ALL
  select DATE('2020-01-15') AS date_day UNION ALL
  select DATE('2020-01-16') AS date_day UNION ALL
  select DATE('2020-02-01') AS date_day UNION ALL
  select DATE('2020-02-02') AS date_day UNION ALL
  select DATE('2020-02-03') AS date_day UNION ALL
  select DATE('2020-02-04') AS date_day UNION ALL
  select DATE('2020-02-05') AS date_day UNION ALL
  select DATE('2020-02-06') AS date_day UNION ALL
  select DATE('2020-02-07') AS date_day UNION ALL
  select DATE('2020-02-08') AS date_day UNION ALL
  select DATE('2020-02-09') AS date_day UNION ALL
  select DATE('2020-02-10') AS date_day UNION ALL
  select DATE('2020-02-11') AS date_day UNION ALL
  select DATE('2020-02-12') AS date_day UNION ALL
  select DATE('2020-02-13') AS date_day UNION ALL
  select DATE('2020-02-14') AS date_day UNION ALL
  select DATE('2020-02-15') AS date_day UNION ALL
  select DATE('2020-02-16') AS date_day

), all_days as (

SELECT
    CAST(date_day AS date) as dt,
    CAST(format_datetime('%Y%m%d', date_day) as int64) as date_key,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day

FROM date_spine

), calculated as (

SELECT
    dt,
    date_key,
    year,
    quarter,
    month,
    day,
    EXTRACT (DAYOFWEEK FROM dt) as day_of_week,
    FORMAT_DATE('%B %d, %Y', dt) as full_date,
    FORMAT_DATE('%b', dt) as month_name,
    FORMAT_DATE('%a', dt) as day_name,
    FORMAT_DATE('%V', dt) as week_of_year,
    FIRST_VALUE(dt) OVER (PARTITION BY year, month ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_day_of_month,
    LAST_VALUE(dt) OVER (PARTITION BY year, month ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_day_of_month,
    DATE_DIFF(DATE_ADD(dt, INTERVAL 1 DAY),DATE_TRUNC(dt, QUARTER), day) as day_of_quarter,
    extract(dayofyear FROM dt) as day_of_year,
    CASE
        WHEN extract(DAYOFWEEK from dt) in (6,7) THEN True
        ELSE FALSE END AS is_weekend

FROM all_days

)

SELECT *
FROM calculated
