select *
from {{ ref('stg_students') }}
where addicted_score < 0 or addicted_score > 10