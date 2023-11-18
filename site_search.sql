create temp function selected_start_date() as (
-- format_date('%Y%m%d', date_sub(current_date('+07'), interval 1 day))
'20230401'
);
create temp function selected_end_date() as (
-- format_date('%Y%m%d', date_sub(current_date('+07'), interval 1 day))
'20230410'
);
create temp function campaign_timeout() as (30);
create temp function query_first_date() as (format_date('%Y%m%d',
date_sub(parse_date('%Y%m%d', selected_start_date()), interval campaign_timeout()
day)));
--pull user_id, session_id, event_name, last event timestamp
with
t0 as (
select
parse_date('%Y%m%d', event_date) as date,
event_timestamp,
user_pseudo_id,
(select value.int_value from unnest(event_params) where key = 'ga_session_id') as
session_id,
event_name,
max(event_timestamp) over (partition by user_pseudo_id,(select value.int_value
from unnest(event_params) where key = 'ga_session_id')) as last_timestamp,
case
when (
select coalesce(value.string_value, cast(value.int_value as string),
cast(value.float_value as string), cast(value.double_value as string))
from unnest(event_params) where key = 'search_term'
) is not null
then (
select coalesce(value.string_value, cast(value.int_value as string),
cast(value.float_value as string), cast(value.double_value as string))
from unnest(event_params) where key = 'search_term'
)
else null
end as search_term,
case
when (select value.string_value from unnest(event_params) where key =
'page_referrer') is not null
then (select value.string_value from unnest(event_params) where key =
'page_referrer') else null
end as page_referrer,
case
when (select value.string_value from unnest(event_params) where key =
'page_location') is not null
then (select value.string_value from unnest(event_params) where key =
'page_location') else null
end as page_location,
FROM `cellphones-306305.analytics_249973737.events_*`
where
regexp_extract(_table_suffix,'[0-9]+') between selected_start_date() and
selected_end_date()
),
-- Caculate result page views & time after search
--filter t0 with event view_search_result and page_view
t1 as (
select
date,
event_timestamp,
user_pseudo_id,
session_id,
event_name,
last_timestamp,
search_term,
page_referrer,
page_location,
row_number() over (partition by user_pseudo_id, session_id order by
event_timestamp desc, event_name desc) as t1_row_number
FROM t0
where
event_name = "view_search_results"
or
event_name = "page_view"
),
--filter result pageviews
t2 as (
select t1.*,
row_number() over (partition by user_pseudo_id, session_id order by event_timestamp
desc, event_name desc) as t2_row_number
from t1
where
(event_name = "view_search_results" and contains_substr(page_location,
"catalogsearch/result"))
or
(event_name = "page_view"
-- and contains_substr(page_location, ".html")
and contains_substr(page_referrer, "catalogsearch/result") --or
-- contains_substr(page_location, "sforum/?s="))
)
),
--calculate result_pageviews, time_after_search, exits, refinements
t3 as (
select
date,
event_timestamp,
user_pseudo_id,
session_id,
event_name,
search_term,
page_referrer,
page_location,
round ((coalesce(lead(event_timestamp) over (partition by user_pseudo_id,
session_id order by event_timestamp asc), last_timestamp) - event_timestamp)/1000000,
0) as time_after_search,
t2_row_number - coalesce(lead(t2_row_number) over (partition by user_pseudo_id,
session_id order by event_timestamp asc),0) - 1 as result_pageviews,
t1_row_number - coalesce(lead(t1_row_number) over (partition by user_pseudo_id,
session_id order by event_timestamp asc),0) - 1 as search_depth,
case
when t1_row_number = 1 then 1
else 0
end as exits,
case when lead(event_timestamp) over (partition by user_pseudo_id, session_id
order by event_timestamp asc) is not null then 1 else 0 end as refinements,
case when search_term = coalesce(lag(search_term) over (partition by
user_pseudo_id, session_id order by event_timestamp asc), "null") then 0 else 1 end as
condition
from t2
where event_name = "view_search_results"
),
--
t4 as (
select
date,
event_timestamp,
user_pseudo_id,
session_id,
event_name,
search_term,
page_referrer,
page_location,
time_after_search,
result_pageviews,
search_depth,
exits,
refinements,
sum(condition) over (partition by user_pseudo_id, session_id order by
event_timestamp asc rows between unbounded preceding and current row) as group_1,
from t3
),
--
t5 as (
select
date,
-- min(event_timestamp) over (partition by user_pseudo_id, session_id, group_1) as
event_timestamp,
user_pseudo_id,
session_id,
search_term,
first_value(page_referrer) over (partition by user_pseudo_id, session_id, group_1
order by event_timestamp asc) as start_page,
first_value(page_location) over (partition by user_pseudo_id, session_id, group_1
order by event_timestamp asc) as destination_page,
sum(time_after_search) over (partition by user_pseudo_id, session_id, group_1) as
time_after_search,
sum(result_pageviews) over (partition by user_pseudo_id, session_id, group_1) as
result_pageviews,
sum(search_depth) over (partition by user_pseudo_id, session_id, group_1) as
pageviews_after_search,
max(exits) over (partition by user_pseudo_id, session_id, group_1) as exits,
min(refinements) over (partition by user_pseudo_id, session_id, group_1) as
refinements,
count(*) over (partition by user_pseudo_id, session_id, group_1) as
search_pageviews,
from t4
),
t6 as (
select date, event_timestamp, user_pseudo_id,
        session_id, search_term, start_page, destination_page,
        time_after_search, result_pageviews, pageviews_after_search, refinements, search_pageviews, exits
from t5
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
)
select * from t6
