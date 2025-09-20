use database raw;

create or replace file format ff_csv_rcm
type = csv
skip_header = 1
field_optionally_enclosed_by = '"';

create or replace file format ff_jsonl_rcm
type = json
strip_outer_array = true;

create or replace stage stg_rcm_elig
url = 's3://healthcare-rcm-raw/raw/eligibility/'
storage_integration = s3_int_rcm
file_format = ff_csv_rcm;

--s3://healthcare-rcm-raw/raw/claims_header/claims_header.jsonl
create or replace stage stg_rcm_hdr
url = 's3://healthcare-rcm-raw/raw/claims_header/'
storage_integration = s3_int_rcm
file_format = ff_jsonl_rcm;

create or replace stage stg_rcm_line
url = 's3://healthcare-rcm-raw/raw/claims_line/'
storage_integration = s3_int_rcm
file_format = ff_csv_rcm;

create or replace stage stg_rcm_remits
url = 's3://healthcare-rcm-raw/raw/remits/'
storage_integration = s3_int_rcm
file_format = ff_csv_rcm;

create or replace stage stg_rcm_providers
url = 's3://healthcare-rcm-raw/raw/providers/'
storage_integration = s3_int_rcm 
file_format = ff_csv_rcm;

create or replace stage stg_rcm_payers
url = 's3://healthcare-rcm-raw/raw/payer_master/'
storage_integration = s3_int_rcm 
file_format = ff_csv_rcm;

 
copy into RAW.ELIG.ELIGIBILITY
from (
  select
    $1::string, $2::string, $3::string, $4::date,  $5::string,
    $6::string, $7::string, $8::string,
    $9::date,   nullif($10,'')::date,
    $11::string, $12::string, $13::string, $14::string,
    $15::string, $16::string,
    metadata$filename, current_timestamp()
  from @stg_rcm_elig (file_format => ff_csv_rcm)
)
file_format = (format_name = ff_csv_rcm);

copy into RAW.CLAIMS.CLAIMS_HEADER
from (
  select
    ($1:claim_id)::string,
    ($1:member_id)::string,
    ($1:payer_id)::string,
    ($1:provider_id)::string,
    ($1:billing_npi)::string,
    ($1:rendering_npi)::string,
    ($1:taxonomy_code)::string,
    ($1:tin)::string,
    ($1:claim_type)::string,
    to_timestamp_ntz(($1:service_from)::string),
    to_timestamp_ntz(($1:service_to)::string),
    ($1:place_of_service)::string,
    ($1:facility_type_code)::string,
    ($1:claim_frequency_code)::string,
    to_timestamp_ntz(($1:claim_submit_date)::string),
    ($1:claim_status)::string,
    try_to_decimal(($1:total_charge)::string,38,2),
    ($1:drg_code)::string,
    ($1:icn)::string,
    ($1:tcn)::string,
    to_timestamp_ntz(($1:updated_at)::string),
    metadata$filename, current_timestamp()
  from @stg_rcm_hdr (file_format => ff_jsonl_rcm)
)
file_format = (format_name = ff_jsonl_rcm);

copy into RAW.CLAIMS.CLAIMS_LINE
from (
  select
    $1::string,$2::number,$3::string,$4::string,$5::number,
    $6::number(38,2),$7::number(38,2),$8::number(38,2),
    nullif($9,'')::string,
    $10::string,$11::string,$12::string,$13::string,$14::string,$15::string,
    $16::string,
    $17::date,$18::date,
    metadata$filename, current_timestamp()
  from @stg_rcm_line (file_format => ff_csv_rcm)
)
file_format = (format_name = ff_csv_rcm);

copy into RAW.CLAIMS.REMITS
from (
  select
    $1::string,$2::string,$3::number,
    $4::number(38,2),nullif($5,'')::string,$6::number(38,2),
    $7::date,$8::string,$9::string,$10::string,
    $11::number(38,2),$12::number(38,2),
    $13::string,$14::string,$15::date,
    $16::string,$17::string,
    metadata$filename, current_timestamp()
  from @stg_rcm_remits (file_format => ff_csv_rcm)
)
file_format = (format_name = ff_csv_rcm);

copy into RAW.PROVIDER.PROVIDERS
from (
  select
    $1::string,$2::string,$3::string,$4::string,$5::string,$6::string,$7::number(1),
    metadata$filename, current_timestamp()
  from @stg_rcm_providers (file_format => ff_csv_rcm)
);

copy into RAW.LOOKUPS.PAYER_MASTER
from (
  select
    $1::string,$2::string,$3::string,
    metadata$filename, current_timestamp()
  from @stg_rcm_payers (file_format => ff_csv_rcm)
);

-- counts
select 'elig' src, count(*) from RAW.ELIG.ELIGIBILITY
union all
select 'hdr' , count(*) from RAW.CLAIMS.CLAIMS_HEADER
union all
select 'line', count(*) from RAW.CLAIMS.CLAIMS_LINE
union all
select 'remt', count(*) from RAW.CLAIMS.REMITS;

--tables
select * from RAW.ELIG.ELIGIBILITY;
select * from RAW.CLAIMS.CLAIMS_HEADER;
select * from RAW.CLAIMS.CLAIMS_LINE;
select * from RAW.CLAIMS.REMITS;