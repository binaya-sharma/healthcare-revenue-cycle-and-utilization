select *
from {{ ref('fact_claims_line') }}
where claim_status = 'Denied'
  and latest_denial_code_final is null;


  