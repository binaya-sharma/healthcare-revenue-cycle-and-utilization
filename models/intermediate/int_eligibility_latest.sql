{{ config(materialized='view') }}

with ranked as (
  select
    history.*,
    row_number() over (
      partition by member_id, payer_id, plan_id
      order by _ingested_at desc, source_filename desc
    ) as rn
  from {{ ref('int_eligibility_history') }} history
)

select
  member_id, payer_id, plan_id,
  plan_name, product_type,
  coverage_start, coverage_end, is_active,
  member_group_id, subscriber_id, metal_tier,
  dob, gender, address_zip, state,
  source_filename, _ingested_at
from ranked
where rn = 1