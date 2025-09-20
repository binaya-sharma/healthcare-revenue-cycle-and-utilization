{{ config(
    materialized = 'incremental',
    unique_key   = 'claim_header_sk'
) }}

with src as (

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
        _ingested_at,
        
        -- surrogate key for uniqueness
        md5(
            claim_id
            || coalesce(claim_status,'')
            || coalesce(total_charge::string,'')
            || coalesce(updated_at::string,'')
        ) as claim_header_sk

    from {{ ref('stg_claims_header') }}   -- staging view
)

select * from src

{% if is_incremental() %}
  -- only new or changed versions
  where claim_header_sk not in (
    select claim_header_sk from {{ this }}
  )
{% endif %}