select
  usage_date,
  account_id,
  service,
  region,
  team,
  env,
  sum(usage_hours) as total_usage_hours,
  sum(cost_usd) as total_cost_usd
from {{ ref('stg_infra_costs') }}
group by 1, 2, 3, 4, 5, 6
