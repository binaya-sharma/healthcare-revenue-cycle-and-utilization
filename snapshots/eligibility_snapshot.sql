{% snapshot eligibility_snapshot %}
  
  {{
    config(
      target_schema = 'RCM_SNAPSHOTS',
      unique_key    = "member_id || '-' || payer_id || '-' || plan_id", 
      strategy      = 'check',
      check_cols    = [
        'plan_name','product_type','metal_tier',
        'member_group_id','subscriber_id',
        'coverage_start','coverage_end',
        'address_zip','state','dob','gender'
      ]
    )
  }}

  select
    {{ dbt_utils.generate_surrogate_key(['member_id','payer_id','plan_id']) }} as e_sk, 
    member_id,
    payer_id,
    plan_id,
    plan_name,
    product_type,
    metal_tier,
    member_group_id,
    subscriber_id,
    coverage_start,
    coverage_end,
    dob,
    gender,
    address_zip,
    state,
    source_filename,
    _ingested_at
  from {{ ref('stg_eligibility') }}

{% endsnapshot %}