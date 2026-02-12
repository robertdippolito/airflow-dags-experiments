select *
from {{ ref('stg_students') }}
where sleep_hours_per_night < 0 or sleep_hours_per_night > 12
