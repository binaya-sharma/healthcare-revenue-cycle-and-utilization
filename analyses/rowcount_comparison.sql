-- Compare row counts between raw, intermediate, and enriched layers\
--Ensures no cross joins, everything matched by dataset name

with raw_counts as (
    select 'eligibility' as dataset, count(*) as raw_count
    from {{ source('raw_elig', 'ELIGIBILITY') }}
    union all
    select 'claims_header', count(*) 
    from {{ source('raw_claims', 'CLAIMS_HEADER') }}
    union all
    select 'claims_line', count(*) 
    from {{ source('raw_claims', 'CLAIMS_LINE') }}
    union all
    select 'remits', count(*) 
    from {{ source('raw_claims', 'REMITS') }}
),

intermediate_counts as (
    select 'eligibility' as dataset, count(*) as int_count
    from {{ ref('int_eligibility_history') }}
    union all
    select 'claims_header', count(*) 
    from {{ ref('int_claims_header_history') }}
    union all
    select 'claims_line', count(*) 
    from {{ ref('int_claims_line_history') }}
    union all
    select 'remits', count(*) 
    from {{ ref('int_remits_history') }}
),

enriched_counts as (
    select 'eligibility' as dataset, count(*) as enriched_count
    from {{ ref('int_eligibility_latest') }}
    union all
    select 'claims_header', count(*) 
    from {{ ref('int_claims_header_latest') }}
    union all
    select 'claims_line', count(*) 
    from {{ ref('int_claims_line_enriched') }}
    union all
    select 'remits', count(*) 
    from {{ ref('int_remits_latest') }}
)

select
    r.dataset,
    r.raw_count,
    i.int_count,
    e.enriched_count,
    r.raw_count - i.int_count as raw_vs_int_diff,
    i.int_count - e.enriched_count as int_vs_enriched_diff
from raw_counts r
left join intermediate_counts i using (dataset)
left join enriched_counts e using (dataset)
order by r.dataset;