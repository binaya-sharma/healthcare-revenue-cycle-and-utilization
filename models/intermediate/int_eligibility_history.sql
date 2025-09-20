{{
  config(
    materialized='incremental',
    unique_key=['member_id','payer_id','plan_id','coverage_start','source_filename'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
  )
}}

with src as (
  select
    member_id,
    payer_id,
    plan_id,
    plan_name,
    product_type,
    cast(coverage_start as date) as coverage_start,
    coalesce(cast(coverage_end as date), to_date('9999-12-31')) as coverage_end,
    case when current_date between coverage_start and coalesce(coverage_end, '9999-12-31')
    then 1 else 0  end as is_active,
    member_group_id,
    subscriber_id,
    metal_tier,
    dob,
    gender,
    address_zip,
    state,
    source_filename,
    _ingested_at
  from {{ ref('stg_eligibility') }}
  --is_incremental load this will merge only new records
  {% if is_incremental() %}
    where _ingested_at >
      (select coalesce(max(_ingested_at), '1900-01-01') from {{ this }})
  {% endif %}

)

select * from src