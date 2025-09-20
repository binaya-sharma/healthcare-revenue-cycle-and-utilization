-- Fails if a claim is marked in-coverage but service_from is outside coverage dates
select *
from {{ ref('fact_claims_line') }}
where in_coverage_at_service = 1
  and (
      service_from::date < coverage_start::date
   or service_from::date > coalesce(coverage_end, '2999-12-31')
  )