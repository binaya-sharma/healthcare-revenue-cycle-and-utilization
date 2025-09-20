{{ config(materialized='view') }}

select
    member_id,
    upper(trim(first_name)) as first_name,
    upper(trim(last_name)) as last_name,
    dob,
    gender,
    plan_id,
    plan_name,
    payer_id,
    coverage_start,
    coverage_end,
    member_group_id,
    subscriber_id,
    product_type,
    metal_tier,
    address_zip,
    upper(trim(state)) as state,
    source_file as source_filename,
    _ingested_at
from {{ source('raw_elig', 'ELIGIBILITY') }}