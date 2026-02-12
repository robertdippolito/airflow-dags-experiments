select *
from {{ ref('stg_students') }}
where mental_health_score < 0 or mental_health_score > 10