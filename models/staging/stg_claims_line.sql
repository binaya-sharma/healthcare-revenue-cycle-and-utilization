{{ config(materialized='view') }}

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
    source_file as source_filename,
    _ingested_at
from {{ source('raw_claims', 'CLAIMS_LINE') }}