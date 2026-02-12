select
  usage_bucket,
  sleep_bucket,
  count(*) as student_count,
  avg(addicted_score) as avg_addicted_score,
  avg(conflicts_over_social_media) as avg_conflicts_over_social_media
from {{ ref('int_students_enriched') }}
group by 1, 2