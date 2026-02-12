drop table if exists infra_demo.monthly_costs;

create table infra_demo.monthly_costs as
with typed as (
  select
    usage_date::date as usage_date,
    account_id,
    service,
    region,
    resource_id,
    usage_hours::numeric(10, 2) as usage_hours,
    cost_usd::numeric(10, 2) as cost_usd,
    team,
    env
  from infra_demo.raw_infra_costs
),
daily as (
  select
    usage_date,
    account_id,
    service,
    region,
    team,
    env,
    sum(usage_hours) as total_usage_hours,
    sum(cost_usd) as total_cost_usd
  from typed
  group by 1, 2, 3, 4, 5, 6
),
monthly as (
  select
    date_trunc('month', usage_date)::date as usage_month,
    account_id,
    team,
    service,
    sum(total_usage_hours) as total_usage_hours,
    sum(total_cost_usd) as total_cost_usd
  from daily
  group by 1, 2, 3, 4
)
select *
from monthly;
