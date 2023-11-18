-- get all data in the specific date range that has event session_start
with sub_data as (
  select user_pseudo_id, PARSE_DATE('%Y%m%d',  event_date) as event_date,
          event_timestamp, event_name
    from `project_id.events_*`
    WHERE _table_suffix BETWEEN '20230326' AND '20230416'
    AND event_name = 'session_start'
),
user_first_week as (
      select user_pseudo_id,
            DATE_TRUNC(min(event_date), WEEK) AS first_week
      from sub_data
      group by 1
),
new_user_by_week as (
      select first_week,
            count(user_pseudo_id) as new_users
      from user_first_week
      group by 1
),
user_retention_by_week as (
      select user_pseudo_id,
      DATE_TRUNC(event_date, WEEK) as retention_week
      from sub_data
      group by 1, 2
),
retained_user_by_week as (
      select user_first_week.first_week,
            user_retention_by_week.retention_week,
            count(user_retention_by_week.user_pseudo_id) as retained_users
      from user_retention_by_week
      left join user_first_week on user_first_week.user_pseudo_id = user_retention_by_week.user_pseudo_id
      group by 1, 2
)

select concat(retained_user_by_week.first_week, " - "  , date_add(retained_user_by_week.first_week, interval 6 day)) as week_day,
    --   retained_user_by_week.retention_week,
      new_user_by_week.new_users,
      retained_user_by_week.retained_users,
      concat('W', DATE_DIFF(retained_user_by_week.retention_week, retained_user_by_week.first_week, WEEK)) as retention_week_no, --ABS(DATE_DIFF(retained_user_by_week.retention_week, retained_user_by_week.first_week, WEEK))
      retained_user_by_week.retained_users/new_user_by_week.new_users as retention_rate
from retained_user_by_week
left join new_user_by_week on retained_user_by_week.first_week = new_user_by_week.first_week
where retained_user_by_week.first_week is not null
order by 1,4
```
