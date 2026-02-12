select *
from {{ ref('stg_infra_costs') }}
where cost_usd < 0
   or usage_hours < 0
