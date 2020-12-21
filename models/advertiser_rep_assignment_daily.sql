{{
  config(
    materialized = "ephemeral"
  )
}}

{% set rep_types = get_distinct_rep_types() %}

WITH source AS (

  select * from {{ ref('stg_bloodmoon__users_accounts') }}

), calendar AS (

  select * FROM {{ ref('calendar') }}

), advertisers AS (

  select * from {{ ref('dim_advertiser_current') }}

), ranked AS (

  select
    source.account_id,
    source.relationship,
    calendar.dt,

    user_id,

    row_number() over (
      partition by
        source.account_id,
        source.relationship,
        calendar.dt
      order by
        coalesce(source.date_added, source.date_assignment_starts) desc,
        user_id desc
    ) AS user_id_rank

  from
    source
    join calendar on
      calendar.dt >= cast(source.date_assignment_starts as date) and
      ( calendar.dt < cast(source.date_assignment_ends as date) or
        source.date_assignment_ends is null)

  where
      calendar.dt <= current_date()
  
), reps_by_date AS (

  select
    ranked.*,
    advertisers.advertiser_id,
    advertisers.parent_company_id

  from
    ranked
    join
      advertisers on
        advertisers.account_id = ranked.account_id

  where
    ranked.user_id_rank = 1

)

select *
from
  reps_by_date
