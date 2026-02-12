select 
    student_id,
    age,
    gender,
    academic_level,
    country,
    avg_daily_usage_hours,
    most_used_platform,
    affects_academic_performance,
    sleep_hours_per_night,
    mental_health_score,
    relationship_status,
    conflicts_over_social_media,
    addicted_score,
    case
        when avg_daily_usage_hours < 2 then 'low'
        when avg_daily_usage_hours <= 5 then 'medium'
        else 'high'
    end as usage_bucket,
    case 
        when sleep_hours_per_night < 6 then 'low'
        when sleep_hours_per_night <= 8 then 'normal'
        else 'high'
    end as sleep_bucket,
    case
        when addicted_score <= 3 then 'low'
        when addicted_score <= 7 then 'medium'
        else 'high'
    end as addiction_risk,
    affects_academic_performance as academic_impact_flag 
from {{ ref('stg_students') }}