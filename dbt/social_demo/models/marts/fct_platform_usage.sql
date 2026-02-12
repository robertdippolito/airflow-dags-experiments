select 
    most_used_platform,
    country,
    academic_level,
    count(*) as student_count,
    avg(avg_daily_usage_hours) as avg_daily_usage_hours,
    avg(addicted_score) as avg_addicted_score
from {{ ref('int_students_enriched') }}
group by 1,2,3