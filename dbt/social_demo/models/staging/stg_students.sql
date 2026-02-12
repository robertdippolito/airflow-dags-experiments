select
    cast(student_id as integer) as student_id,
    cast(age as integer) as age,
    trim(gender) as gender,
    trim(academic_level) as academic_level,
    trim(country) as country,
    cast(avg_daily_usage_hours as numeric(5, 2)) as avg_daily_usage_hours,
    trim(most_used_platform) as most_used_platform,
    case
        when lower(trim(affects_academic_performance)) = 'yes' then true
        when lower(trim(affects_academic_performance)) = 'no' then false
        else null
    end as affects_academic_performance,
    cast(sleep_hours_per_night as numeric(4, 2)) as sleep_hours_per_night,
    cast(mental_health_score as integer) as mental_health_score,
    trim(relationship_status) as relationship_status,
    cast(conflicts_over_social_media as integer) as conflicts_over_social_media,
    cast(addicted_score as integer) as addicted_score
from {{ ref('students_social_media_addiction') }}