{{ config(
    materialized='incremental',
    unique_key='remit_sk',
    on_schema_change='sync_all_columns'
) }}

with src as (
  select
    remit_id,
    claim_id,
    line_num,
    paid_amount,
    allowed_amount,
    adj_amount,
    denial_code,
    remit_dt,
    group_code,
    remark_code,
    reason_description,
    billed_amount,
    payment_id,
    payment_method,
    payment_dt,
    payer_claim_control_number,
    trace_number,
    source_filename,
    _ingested_at,

    -- surrogate key captures “this version of this remit record”
    md5(
      coalesce(remit_id,'')
      || coalesce(claim_id,'')
      || coalesce(line_num::string,'')
      || coalesce(remit_dt::string,'')
      || coalesce(paid_amount::string,'')
      || coalesce(allowed_amount::string,'')
      || coalesce(adj_amount::string,'')
      || coalesce(_ingested_at::string,'')
    ) as remit_sk
  from {{ ref('stg_remits') }}   -- Bronze view
)

select * from src
{% if is_incremental() %}
  where remit_sk not in (select remit_sk from {{ this }})
{% endif %}