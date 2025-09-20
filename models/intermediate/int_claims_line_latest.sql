{{ config(materialized='view') }}

with ranked as (
    select
        l.*,
        row_number() over (
            partition by claim_id, line_num
            order by _ingested_at desc
        ) as rn
    from {{ ref('int_claims_line_history') }} l
)

select
    claim_id,
    line_num,
    cpt_code,
    icd_code,
    units,
    charge_amount,
    allowed_amount,
    paid_amount,
    denial_code,
    modifier1,
    modifier2,
    modifier3,
    modifier4,
    revenue_code,
    pos_code,
    rendering_npi,
    service_from,
    service_to,
    source_filename,
    _ingested_at
from ranked
where rn = 1