-- Fails if remit date exists but amounts are all null
select *
from {{ ref('fact_claims_line') }}
where last_remit_dt_final is not null
  and coalesce(allowed_amount_final, paid_amount_final, adjustment_amount_final) is null