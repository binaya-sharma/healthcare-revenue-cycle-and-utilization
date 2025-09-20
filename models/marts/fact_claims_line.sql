{{ config(
  materialized='incremental',
  schema='rcm_marts',
  unique_key=['claim_id','line_num'],
  incremental_strategy='merge',
  on_schema_change='sync_all_columns'
) }}

with base as (
  select
    l.claim_id,
    l.line_num,
    l.member_id,
    l.payer_id,
    l.provider_id,
    l.provider_npi,
    l.provider_taxonomy,
    l.provider_specialty,
    l.cpt_code,
    l.icd_code,
    l.modifier1, l.modifier2, l.modifier3, l.modifier4,
    l.revenue_code, l.pos_code, l.rendering_npi,
    l.claim_type, l.claim_status,
    l.service_from, l.service_to,
    l.units,

    -- billed intent from claims line
    l.line_charge_amount as billed_amount,
    l.allowed_amount as allowed_amount_claim,
    l.paid_amount as paid_amount_claim,
    l.adjustment_amount as adjustment_amount_claim,
    l.latest_denial_code as latest_denial_code_claim,
    l.last_remit_dt as last_remit_dt_claim,
    l.in_coverage_at_service,
    l.plan_id, l.product_type,
    l.coverage_start, l.coverage_end,
    l.source_filename,
    l._ingested_at_hwm
  from {{ ref('int_claims_line_enriched') }} l
),

remit_latest as (
  select
    claim_id, line_num,
    paid_amount as paid_amount_remit,
    allowed_amount as allowed_amount_remit,
    adj_amount as adjustment_amount_remit,
    denial_code as latest_denial_code_remit,
    remit_dt as last_remit_dt_remit
  from {{ ref('int_remits_latest') }}
),

-- grain: one row per (claim_id, line_num)
joined as (
  select
    b.*,
    coalesce(r.allowed_amount_remit,b.allowed_amount_claim) as allowed_amount_final,
    coalesce(r.paid_amount_remit,b.paid_amount_claim) as paid_amount_final,
    coalesce(r.adjustment_amount_remit,b.adjustment_amount_claim) as adjustment_amount_final,
    coalesce(r.latest_denial_code_remit,b.latest_denial_code_claim) as latest_denial_code_final,
    coalesce(r.last_remit_dt_remit,b.last_remit_dt_claim) as last_remit_dt_final
  from base b
  left join remit_latest r
    on r.claim_id = b.claim_id
   and r.line_num = b.line_num
),

elig_join as (
  select
    j.*,
    m.e_sk as e_sk
  from joined j
  left join {{ ref('dim_member_scd2') }} m
    on m.member_id = j.member_id
   and m.payer_id  = j.payer_id
   and m.plan_id   = j.plan_id
   and j.service_from::date between m.cov_valid_from and m.cov_valid_to
)

select * from elig_join

{% if is_incremental() %}
where _ingested_at_hwm >
  (select coalesce(max(_ingested_at_hwm),'1900-01-01'::timestamp) from {{ this }})
{% endif %}