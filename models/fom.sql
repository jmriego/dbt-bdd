{{
  config(
    materialized = "table"
  )
}}


WITH calendar AS (

  select * from {{ ref('calendar') }}

), days_per_month as (

SELECT
    first_day_of_month,
    count(*) AS count_days

FROM calendar
GROUP BY 1

)

SELECT * FROM days_per_month
