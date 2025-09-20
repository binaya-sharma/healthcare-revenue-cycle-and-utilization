{{ config(materialized='view') }}

select
    remit_id,
    claim_id,
    line_num,
    paid_amount,
    denial_code,
    adj_amount,
    remit_dt,
    group_code,
    remark_code,
    reason_description,
    allowed_amount,
    billed_amount,
    payment_id,
    payment_method,
    payment_dt,
    payer_claim_control_number,
    trace_number,
    source_file as source_filename,
    _ingested_at
from {{ source('raw_claims', 'REMITS') }}