--get user_id, session_id, event_name, last event timestamp
with base_tb as (
    select parse_date('%Y%m%d', event_date) as date,
        event_timestamp,
        user_pseudo_id,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id') as session_id,
        event_name,
        max(event_timestamp) over (partition by user_pseudo_id,
        (select value.int_value from unnest(event_params) where key = 'ga_session_id')) as last_timestamp,
        case 
            when (select coalesce(value.string_value, cast(value.int_value as string), cast(value.float_value as string), cast(value.double_value as string)) from unnest(event_params) 
                    where key = 'search_term') is not null 
                    then (select coalesce(value.string_value, cast(value.int_value as string), cast(value.float_value as string), cast(value.double_value as string))
                    from unnest(event_params) where key = 'search_term') else null end as search_term,
        case
            when (select value.string_value from unnest(event_params) where key = 'page_referrer') is not null
                    then (select value.string_value from unnest(event_params) where key = 'page_referrer') else null end as page_referrer,
        case
            when (select value.string_value from unnest(event_params) where key = 'page_location') is not null
                    then (select value.string_value from unnest(event_params) where key = 'page_location') else null end as page_location,
    from `project_id.events_*`
    where regexp_extract(_table_suffix,'[0-9]+') between [start_date] and [end_date]
),
-- calculate result page views, time after search
-- filter base on event view_search_result, page_view
t1 as (
    select date, event_timestamp, user_pseudo_id, session_id, event_name, last_timestamp,
        search_term, page_referrer, page_location,
        row_number() over (partition by user_pseudo_id, session_id order by event_timestamp desc, event_name desc) as t1_row_number
    from base_tb
    where event_name = "view_search_results" or event_name = "page_view"
),
-- filter event_name contains view_search_results
t2 as (
    select t1.*,
        row_number() over (partition by user_pseudo_id, session_id order by event_timestamp desc, event_name desc) as t2_row_number
    from t1
    where (event_name = "view_search_results" and contains_substr(page_location, "search/result"))
        or (event_name = "page_view" and contains_substr(page_referrer, "search/result"))
),

--calculate result_pageviews, time_after_search, exits, refinements
t3 as (
    select date, event_timestamp, user_pseudo_id, session_id, event_name,
        search_term, page_referrer, page_location,
        round ((coalesce(lead(event_timestamp) over (partition by user_pseudo_id, session_id order by event_timestamp asc), last_timestamp) - event_timestamp)/1000000, 0) as time_after_search,
        t2_row_number - coalesce(lead(t2_row_number) over (partition by user_pseudo_id,
        session_id order by event_timestamp asc),0) - 1 as result_pageviews,
        t1_row_number - coalesce(lead(t1_row_number) over (partition by user_pseudo_id, session_id order by event_timestamp asc),0) - 1 as search_depth,
    case
        when t1_row_number = 1 then 1 else 0 end as exits,
    case 
        when lead(event_timestamp) over (partition by user_pseudo_id, session_id order by event_timestamp asc) is not null then 1 else 0 end as refinements,
    case 
        when search_term = coalesce(lag(search_term) over (partition by user_pseudo_id, session_id order by event_timestamp asc), "null") then 0 else 1 end as lag_search_term
    from t2
    where event_name = "view_search_results"
),
--
t4 as (
    select date, event_timestamp, user_pseudo_id, session_id, event_name,
            search_term, page_referrer, page_location, time_after_search,
            result_pageviews, search_depth, exits, refinements,
            sum(condition) over (partition by user_pseudo_id, session_id order by event_timestamp asc rows between unbounded preceding and current row) as lag_search_term,
    from t3
),

t5 as (
    select date, event_timestamp, user_pseudo_id, session_id, search_term,
            first_value(page_referrer) over (partition by user_pseudo_id, session_id, lag_search_term order by event_timestamp asc) as start_page,
            first_value(page_location) over (partition by user_pseudo_id, session_id, lag_search_term order by event_timestamp asc) as destination_page,
            sum(time_after_search) over (partition by user_pseudo_id, session_id, lag_search_term) as time_after_search,
            sum(result_pageviews) over (partition by user_pseudo_id, session_id, lag_search_term) as result_pageviews,
            sum(search_depth) over (partition by user_pseudo_id, session_id, lag_search_term) as pageviews_after_search,
            max(exits) over (partition by user_pseudo_id, session_id, lag_search_term) as exits,
            min(refinements) over (partition by user_pseudo_id, session_id, lag_search_term) as refinements,
            count(*) over (partition by user_pseudo_id, session_id, lag_search_term) as search_pageviews,
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
