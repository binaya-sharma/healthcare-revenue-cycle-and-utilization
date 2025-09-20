{{ config(
    materialized='table',
    schema='rcm_marts'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['member_id','payer_id','plan_id','dbt_valid_from']) }} as e_sk,
    member_id,
    payer_id,
    plan_id,
    plan_name,
    product_type,
    metal_tier,
    member_group_id,
    subscriber_id,
    dob,
    gender,
    address_zip,
    state,
    dbt_valid_from as scd_valid_from, --from dbt snapshot
    
    coalesce(dbt_valid_to, '9999-12-31'::timestamp) as scd_valid_to,
    case when dbt_valid_to is null then true else false end as is_current,
    
    coverage_start as cov_valid_from,
    coalesce(coverage_end, '9999-12-31'::date) as cov_valid_to,

    source_filename,
    _ingested_at
from {{ ref('eligibility_snapshot') }}