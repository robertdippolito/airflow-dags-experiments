drop table if exists social_stage.stg_students;
drop table if exists social_int.int_students_enriched;
drop table if exists social_mart.fct_platform_usage;
drop table if exists social_mart.fct_addiction_by_demographic;
drop table if exists social_mart.fct_sleep_vs_usage;

create schema if not exists social_stage;
create schema if not exists social_int;
create schema if not exists social_mart;

create table social_stage.stg_students as 
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
from social_raw.students_social_media;

create table social_int.int_students_enriched as 
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
from social_stage.stg_students;

create table social_mart.fct_platform_usage as
select 
    most_used_platform,
    country,
    academic_level,
    count(*) as student_count,
    avg(avg_daily_usage_hours) as avg_daily_usage_hours,
    avg(addicted_score) as avg_addicted_score
from social_int.int_students_enriched
group by 1,2,3;

create table social_mart.fct_addiction_by_demographic as 
select 
    gender,
    academic_level,
    relationship_status,
    count(*) as student_count,
    avg(addicted_score) as avg_addicted_score,
    avg(mental_health_score) as avg_mental_health_score
from social_int.int_students_enriched
group by 1,2,3;

create table social_mart.fct_sleep_vs_usage as
select
  usage_bucket,
  sleep_bucket,
  count(*) as student_count,
  avg(addicted_score) as avg_addicted_score,
  avg(conflicts_over_social_media) as avg_conflicts_over_social_media
from social_int.int_students_enriched
group by 1, 2;
