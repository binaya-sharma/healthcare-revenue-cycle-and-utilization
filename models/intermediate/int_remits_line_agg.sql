{{ config(
    materialized='table',
    on_schema_change='sync_all_columns'
) }}

select
  claim_id,
  line_num,
  max(remit_dt) as last_remit_dt,
  sum(coalesce(allowed_amount, 0)) as allowed_amount_sum,
  sum(coalesce(paid_amount, 0)) as paid_amount_sum,
  sum(coalesce(adj_amount, 0)) as adj_amount_sum,
  max_by(denial_code, coalesce(remit_dt, '1900-01-01')) as latest_denial_code,
  max(_ingested_at)                             as last_ingested_at
from {{ ref('int_remits_history') }}
group by 1,2