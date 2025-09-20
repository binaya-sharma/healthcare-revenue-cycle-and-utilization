use role SYSADMIN;
create database if not exists RAW;
create schema   if not exists RAW.ELIG;
create schema   if not exists RAW.CLAIMS;
create schema   if not exists RAW.PROVIDER;
create schema   if not exists RAW.LOOKUPS;

--elig
create or replace table RAW.ELIG.ELIGIBILITY (
  member_id string,
  first_name string,
  last_name string,
  dob date,
  gender string,
  plan_id string,
  plan_name string,
  payer_id string,
  coverage_start date,
  coverage_end date,
  member_group_id string,
  subscriber_id string,
  product_type string,
  metal_tier string,
  address_zip string,
  state string,
  source_file string,
  _ingested_at timestamp_ntz
);


create or replace table RAW.CLAIMS.CLAIMS_HEADER (
  claim_id string,
  member_id string,
  payer_id string,
  provider_id string,
  billing_npi string,
  rendering_npi string,
  taxonomy_code string,
  tin string,
  claim_type string,
  service_from timestamp_ntz,
  service_to timestamp_ntz,
  place_of_service string,
  facility_type_code string,
  claim_frequency_code string,
  claim_submit_date timestamp_ntz,
  claim_status string,
  total_charge number(38,2),
  drg_code string,
  icn string,
  tcn string,
  updated_at timestamp_ntz,
  source_file string,
  _ingested_at timestamp_ntz
);

create or replace table RAW.CLAIMS.CLAIMS_LINE (
  claim_id string,
  line_num number,
  cpt_code string,
  icd_code string,
  units number,
  charge_amount number(38,2),
  allowed_amount number(38,2),
  paid_amount number(38,2),
  denial_code string,
  modifier1 string,
  modifier2 string,
  modifier3 string,
  modifier4 string,
  revenue_code string,
  pos_code string,
  rendering_npi string,
  service_from date,
  service_to date,
  source_file string,
  _ingested_at timestamp_ntz
);

create or replace table RAW.CLAIMS.REMITS (
  remit_id string,
  claim_id string,
  line_num number,
  paid_amount number(38,2),
  denial_code string,
  adj_amount number(38,2),
  remit_dt date,
  group_code string,
  remark_code string,
  reason_description string,
  allowed_amount number(38,2),
  billed_amount number(38,2),
  payment_id string,
  payment_method string,
  payment_dt date,
  payer_claim_control_number string,
  trace_number string,
  source_file string,
  _ingested_at timestamp_ntz
);

create or replace table RAW.PROVIDER.PROVIDERS (
  provider_id string,
  npi string,
  tin string,
  taxonomy string,
  specialty string,
  location_id string,
  active_flag number(1),
  source_file string,
  _ingested_at timestamp_ntz
);

create or replace table RAW.LOOKUPS.PAYER_MASTER (
  payer_id string,
  payer_name string,
  payer_group string,
  source_file string,
  _ingested_at timestamp_ntz
);