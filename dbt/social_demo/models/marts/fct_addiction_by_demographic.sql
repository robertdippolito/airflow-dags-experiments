select 
    gender,
    academic_level,
    relationship_status,
    count(*) as student_count,
    avg(addicted_score) as avg_addicted_score,
    avg(mental_health_score) as avg_mental_health_score
from {{ ref('int_students_enriched') }}
group by 1,2,3