-- ClickHouse
-- Calculate z-score for number of messages sent per day

with 

sends as
(
    select
        d.date,
        coalesce(s.count_sends, 0) as count_sends
    from
    (
        select today() - 1 - 365 + number as date 
        from numbers(366)
    ) d

    left join

    (
        select
          timestamp,
          countIfMerge(sends) as count_sends
        from stats.message_statistics
        where account_id = 111
        and platform = 1
        group by timestamp
        order by timestamp
    ) s
    on s.timestamp = d.date
),

bounds as
(
    select quantilesExact(0.015, 0.985)(count_sends) as b -- Use trim mean and std
    from sends
)

select  
    -- date,
    (count_sends - trim_avg) / (if(trim_std != 0, trim_std, 1)) as trim_z_score
from
(
    select
      date,
      count_sends,
      (select b[1] from bounds) as lower_bound,
      (select b[2] from bounds) as upper_bound,
      avgIf(count_sends, (count_sends between lower_bound and upper_bound)) over w as trim_avg,
      stddevSampIf(count_sends, (count_sends between lower_bound and upper_bound)) over w as trim_std
    from sends
    window w as (order by date rows between 90 PRECEDING and 1 PRECEDING)
    order by date desc
    limit 7 -- days
)
order by date desc