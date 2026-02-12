select
  date_trunc('month', usage_date)::date as usage_month,
  account_id,
  team,
  service,
  sum(total_usage_hours) as total_usage_hours,
  sum(total_cost_usd) as total_cost_usd
from {{ ref('int_daily_costs') }}
group by 1, 2, 3, 4
