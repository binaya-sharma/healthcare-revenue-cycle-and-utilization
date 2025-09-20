{{ config(materialized='view') }}

with ranked as (
  select
    h.*,
    row_number() over (
      partition by claim_id, line_num
      order by remit_dt desc nulls last, _ingested_at desc
    ) as rn
  from {{ ref('int_remits_history') }} h
)
select
  remit_id,
  claim_id,
  line_num,
  paid_amount,
  allowed_amount,
  adj_amount,
  denial_code,
  remit_dt,
  group_code,
  remark_code,
  reason_description,
  billed_amount,
  payment_id,
  payment_method,
  payment_dt,
  payer_claim_control_number,
  trace_number,
  source_filename,
  _ingested_at
from ranked
where rn = 1 --this is not needed 