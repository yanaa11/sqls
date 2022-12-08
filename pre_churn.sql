select 
    user_id,
    state
from 
(
    select 
        user_id,
        groupUniqArray(device) as user_devices,
        floor(median(days_between_events)) as fi,
        max(nth_event) as n_events,
        date_diff('day', max(day), toStartOfDay(now())) as days_since_last_event,
        intDiv(days_since_last_event, fi) as si,
        multiIf(
        
            ((fi <= 3) and (si between 3 and 4)), 'pre_churn',
            ((fi <= 3) and (si >= 5)), 'churn',
            
            ((fi between 4 and 7) and (si = 3)), 'pre_churn',
            ((fi between 4 and 7) and (si >= 4)), 'churn',
            
            ((fi between 8 and 19) and (si = 2)), 'pre_churn',
            ((fi between 8 and 19) and (si >= 3)), 'churn',
            
            ((fi >= 20) and (si = 1)), 'pre_churn',
            ((fi >= 20) and (si >= 2)), 'churn',
            
            'active'
            
        ) as state,
        groupArray(day) as times,
        groupArray(days_between_events) as intervals
    from 
    (
        select 
            user_id,
            device,
            day,
            any(day) over (partition by user_id order by day asc rows between 1 preceding and 1 preceding) as prev_event_day, -- , or following for lead
            row_number() over (partition by user_id order by day asc) as nth_event,
            date_diff('day', prev_event_day, day) as days_between_events
        from 
        (
            select distinct 
                user_id,
                toStartOfDay(time) as day,
                any(device_id) as device
            from some_users_actions_log
            where account_id = {{acc_id}}
            and application_code = '{{app_code}}'
            and event_name = '{{event_name}}'
            group by user_id, toStartOfDay(time)
            order by user_id, day asc
        )
        order by user_id, day asc
    )
    where nth_event > 1
    group by user_id
)
