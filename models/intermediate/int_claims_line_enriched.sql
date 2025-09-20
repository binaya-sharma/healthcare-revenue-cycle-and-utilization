{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['claim_id','line_num'],
    on_schema_change='sync_all_columns'
  )
}}

with
-- latest snapshots
hdr as (
  select *
  from {{ ref('int_claims_header_latest') }}
),
ln as (
  select *
  from {{ ref('int_claims_line_latest') }}
),
elig as (
  select *
  from {{ ref('int_eligibility_latest') }}
),
rmt as (
  select *
  from {{ ref('int_remits_line_agg') }}
),
prov as (
  select provider_id, npi, taxonomy, specialty
  from {{ ref('providers') }} --lookup table
)

select
  -- keys
  ln.claim_id,
  ln.line_num,

  -- member / payer / provider
  hdr.member_id,
  hdr.payer_id,
  hdr.provider_id,
  prov.npi as provider_npi,
  prov.taxonomy as provider_taxonomy,
  prov.specialty as provider_specialty,

  -- coding / service context
  ln.cpt_code,
  ln.icd_code,
  ln.modifier1,
  ln.modifier2,
  ln.modifier3,
  ln.modifier4,
  ln.revenue_code,
  ln.pos_code,
  ln.rendering_npi,
  hdr.claim_type,
  hdr.claim_status,
  hdr.service_from,
  hdr.service_to,

  -- eligibility snapshot
  elig.plan_id,
  elig.product_type,
  elig.coverage_start,
  elig.coverage_end,
  case
    when hdr.service_from between elig.coverage_start and elig.coverage_end then 1 else 0
  end as in_coverage_at_service,

  -- financials (prefer remit rollups; fall back to line values)
  ln.units,
  ln.charge_amount as line_charge_amount,
  coalesce(rmt.allowed_amount_sum, ln.allowed_amount) as allowed_amount,
  coalesce(rmt.paid_amount_sum, ln.paid_amount) as paid_amount,
  coalesce(rmt.adj_amount_sum, 0) as adjustment_amount,
  coalesce(rmt.latest_denial_code, null) as latest_denial_code,
  rmt.last_remit_dt,

  -- derived finance
  greatest(
    coalesce(rmt.allowed_amount_sum, ln.allowed_amount) - coalesce(rmt.paid_amount_sum, ln.paid_amount),
    0
  ) as patient_responsibility_est,

  case
    when coalesce(rmt.paid_amount_sum, ln.paid_amount) = 0 and coalesce(rmt.latest_denial_code,'') <> '' then 1 else 0
  end as denial_flag,

  -- lineage / freshness
  ln.source_filename,
  -- keep a high-watermark ingested_at for incremental filters/debug
  greatest(
    coalesce(ln._ingested_at, to_timestamp_ntz('1900-01-01')),
    coalesce(hdr._ingested_at, to_timestamp_ntz('1900-01-01')),
    coalesce(elig._ingested_at, to_timestamp_ntz('1900-01-01'))
  ) as _ingested_at_hwm

from ln
join hdr
  on ln.claim_id = hdr.claim_id --latest header record for this claim by _updaed_at

left join elig
  on hdr.member_id = elig.member_id
 and hdr.payer_id  = elig.payer_id --latest header record for this claim by _updaed_at

left join rmt
  on ln.claim_id = rmt.claim_id
 and ln.line_num = rmt.line_num -- claim id and line is always unique

left join prov
  on hdr.provider_id = prov.provider_id --lookps table

{% if is_incremental() %}
-- only re-process rows that are new/changed since our last build
where greatest(
        coalesce(ln._ingested_at,to_timestamp_ntz('1900-01-01')),
        coalesce(hdr._ingested_at,to_timestamp_ntz('1900-01-01')),
        coalesce(elig._ingested_at,to_timestamp_ntz('1900-01-01'))
      )
      > (select coalesce(max(_ingested_at_hwm),to_timestamp_ntz('1900-01-01')) from {{ this }})
{% endif %}
-- this ensures we re-process any rows that might have changed since our last run