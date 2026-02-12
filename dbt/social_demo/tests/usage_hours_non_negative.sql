select *
from {{ ref('stg_students') }}
where avg_daily_usage_hours < 0