{{ config(materialized='view') }}

with ranked as (
    select
        h.*,
        row_number() over (
            partition by claim_id
            order by updated_at desc, _ingested_at desc
        ) as rn
    from {{ ref('int_claims_header_history') }} h
)

select
    claim_id,
    member_id,
    payer_id,
    provider_id,
    billing_npi,
    rendering_npi,
    taxonomy_code,
    tin,
    claim_type,
    service_from,
    service_to,
    place_of_service,
    facility_type_code,
    claim_frequency_code,
    claim_submit_date,
    claim_status,
    total_charge,
    drg_code,
    icn,
    tcn,
    updated_at,
    source_filename,
    _ingested_at
from ranked
where rn = 1