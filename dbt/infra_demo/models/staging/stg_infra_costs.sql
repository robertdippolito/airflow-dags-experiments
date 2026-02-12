select
  cast(usage_date as date) as usage_date,
  account_id,
  service,
  region,
  resource_id,
  cast(usage_hours as numeric(10, 2)) as usage_hours,
  cast(cost_usd as numeric(10, 2)) as cost_usd,
  team,
  env
from {{ ref('infra_costs') }}
