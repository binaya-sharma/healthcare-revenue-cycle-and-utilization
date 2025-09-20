{{ config(
    materialized = 'incremental',
    unique_key   = 'claim_line_sk'
) }}

with src as (

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
        _ingested_at,

        md5(
            claim_id
            || coalesce(line_num::string,'')
            || coalesce(paid_amount::string,'')
            || coalesce(charge_amount::string,'')
            || coalesce(_ingested_at::string,'')
        ) as claim_line_sk

    from {{ ref('stg_claims_line') }}   -- staging view
)

select * from src

{% if is_incremental() %}
  where claim_line_sk not in (
    select claim_line_sk from {{ this }}
  )
{% endif %}