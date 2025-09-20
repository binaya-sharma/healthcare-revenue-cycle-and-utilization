-- =========================================================
-- Core KPIs for Healthcare RCM (facts + SCD2 eligibility)
-- Uses: RCM.RCM_rcm_marts.fact_claims_line, RCM.RCM_rcm_marts.dim_member_scd2
-- Run: dbt compile  → open the compiled SQL under target/compiled/... 
--      or copy/paste sections into Snowflake Worksheets. --see example in docs/kpi's.sql
-- =========================================================

-- 1) Overall Denial Rate
-- ---------------------------------------------------------
select
  round(100.0 * count_if(claim_status = 'Denied' or latest_denial_code_final is not null) / count(*), 2) as denial_rate_pct,
  count_if(claim_status = 'Denied' or latest_denial_code_final is not null) as denied_lines,
  count(*) as total_lines
from RCM.RCM_rcm_marts.fact_claims_line;

-- 2) Paid-to-Billed Ratio (Overall)
-- ---------------------------------------------------------
select
  sum(billed_amount) as total_billed,
  sum(paid_amount_final) as total_paid,
  round(100.0 * sum(paid_amount_final) / nullif(sum(billed_amount), 0), 2) as paid_to_billed_pct
from RCM.RCM_rcm_marts.fact_claims_line;

-- 3) Payer Performance (Paid/Billed + Denial Rate)
-- ---------------------------------------------------------
select
  payer_id,
  sum(billed_amount) as total_billed,
  sum(paid_amount_final) as total_paid,
  round(100.0 * sum(paid_amount_final) / nullif(sum(billed_amount), 0), 2) as paid_to_billed_pct,
  round(100.0 * count_if(claim_status = 'Denied') / count(*), 2) as denial_rate_pct,
  count(*) as claim_lines
from RCM.RCM_rcm_marts.fact_claims_line
group by payer_id
order by denial_rate_pct desc;

-- 4) Paid Amounts by Eligibility Status (SCD2 is_current)
-- ---------------------------------------------------------
select
  case when m.is_current = true then 'Active' else 'Inactive' end as eligibility_status,
  sum(f.paid_amount_final) as total_paid,
  count(*) as claim_lines
from RCM.RCM_rcm_marts.fact_claims_line f
join RCM.RCM_rcm_marts.dim_member_scd2 m
  on f.e_sk = m.e_sk
group by 1;

-- 5) Product Type Comparison (HMO/PPO/EPO)
-- ---------------------------------------------------------
select
  m.product_type,
  sum(f.billed_amount) as total_billed,
  sum(f.paid_amount_final) as total_paid,
  round(100.0 * sum(f.paid_amount_final) / nullif(sum(f.billed_amount), 0), 2) as paid_to_billed_pct,
  count(*) as claim_lines
from RCM.RCM_rcm_marts.fact_claims_line f
join RCM.RCM_rcm_marts.dim_member_scd2 m
  on f.e_sk = m.e_sk
group by m.product_type
order by paid_to_billed_pct desc;

-- 6) High-Cost Members (Top 20 by Paid)
-- ---------------------------------------------------------
select
  f.member_id,
  sum(f.paid_amount_final) as total_paid,
  count(distinct f.claim_id) as claims_count,
  count(*) as claim_lines
from RCM.RCM_rcm_marts.fact_claims_line f
group by f.member_id
order by total_paid desc
limit 20;

-- 7) Top Specialties by Paid Amount
-- ---------------------------------------------------------
select
  provider_specialty,
  sum(paid_amount_final) as total_paid,
  count(*) as claim_lines
from RCM.RCM_rcm_marts.fact_claims_line
group by provider_specialty
order by total_paid desc;

-- 8) Denial Rate by Provider Specialty
-- ---------------------------------------------------------
select
  provider_specialty,
  round(100.0 * count_if(claim_status = 'Denied') / count(*), 2) as denial_rate_pct,
  count(*) as claim_lines
from RCM.RCM_rcm_marts.fact_claims_line
group by provider_specialty
order by denial_rate_pct desc;

-- 9) CPT/ICD Utilization (Top 20 by Paid)
-- ---------------------------------------------------------
select
  cpt_code,
  icd_code,
  count(*) as line_count,
  sum(paid_amount_final) as total_paid
from RCM.RCM_rcm_marts.fact_claims_line
group by cpt_code, icd_code
order by total_paid desc
limit 20;

-- 10) Monthly Paid vs Billed (Trend)
-- ---------------------------------------------------------
select
  date_trunc(month, service_from) as service_month,
  sum(billed_amount) as total_billed,
  sum(paid_amount_final) as total_paid,
  round(100.0 * sum(paid_amount_final) / nullif(sum(billed_amount), 0), 2) as paid_to_billed_pct,
  count(*) as claim_lines
from RCM.RCM_rcm_marts.fact_claims_line
group by service_month
order by service_month;

-- 11) Denial Trend Over Time (Monthly)
-- ---------------------------------------------------------
select
  date_trunc(month, service_from) as service_month,
  count_if(claim_status = 'Denied') as denied_lines,
  count(*) as total_lines,
  round(100.0 * count_if(claim_status = 'Denied') / count(*), 2) as denial_rate_pct
from RCM.RCM_rcm_marts.fact_claims_line
group by service_month
order by service_month;

-- 12) Days to Payment (Service → Last Remit)
-- ---------------------------------------------------------
select
  avg(datediff(day, service_from, last_remit_dt_final)) as avg_days_to_payment,
  percentile_cont(0.5) within group (order by datediff(day, service_from, last_remit_dt_final)) as p50_days_to_payment,
  percentile_cont(0.9) within group (order by datediff(day, service_from, last_remit_dt_final)) as p90_days_to_payment
from RCM.RCM_rcm_marts.fact_claims_line
where last_remit_dt_final is not null;

-- 13) Eligibility Mismatch Rate (claims with NULL E_SK)
-- ---------------------------------------------------------
select
  round(100.0 * count_if(e_sk is null) / count(*), 2) as pct_eligibility_mismatch,
  count_if(e_sk is null) as lines_no_eligibility_match,
  count(*) as total_lines
from RCM.RCM_rcm_marts.fact_claims_line;

-- 14) Eligibility Gaps Breakdown (before start vs after end)
-- ---------------------------------------------------------
with f as (
  select
    member_id, payer_id, plan_id,
    service_from::date as svc_dt
  from RCM.RCM_rcm_marts.fact_claims_line
  where e_sk is null
),
m as (
  select member_id, payer_id, plan_id, cov_valid_from, cov_valid_to
  from RCM.RCM_rcm_marts.dim_member_scd2
  group by 1,2,3,4,5
)
select
  case
    when exists (
      select 1
      from m
      where m.member_id = f.member_id
        and m.payer_id  = f.payer_id
        and m.plan_id   = f.plan_id
        and f.svc_dt < m.cov_valid_from
    ) then 'service_before_coverage'
    when exists (
      select 1
      from m
      where m.member_id = f.member_id
        and m.payer_id  = f.payer_id
        and m.plan_id   = f.plan_id
        and f.svc_dt > m.cov_valid_to
    ) then 'service_after_coverage'
    else 'no_matching_keys'
  end as mismatch_reason,
  count(*) as lines
from f
group by 1
order by lines desc;