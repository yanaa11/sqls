-- ClickHouse

with users as
(
    select 
        user_id,
        groupUniqArray(device_id) as hwids,
        max(date) as last_event_date,
        today() - max(date) as recency,
        count(user_id) as frequency,
        sum(JSONExtractString(event_attributes, 'money_event_attribute_name')::float) as monetary
    from default.user_event_history
    where application_id = 'some_application_id'
    and event_name = 'purchase_event_name'
    and JSONHas(event_attributes, 'money_event_attribute_name') = 1
    group by user_id
),

thr as 
(
    select
        quantile(0.33)(recency) as r_thr0,
        quantile(0.66)(recency) as r_thr1,
        quantile(0.33)(frequency) as f_thr0,
        quantile(0.66)(frequency) as f_thr1,
        quantile(0.33)(monetary) as m_thr0,
        quantile(0.66)(monetary) as m_thr1
    from users
)

select
    user_id,
    hwids,
    last_event_date,
    recency,
    multiIf((recency <= (select r_thr0 from thr)), 3,
            (recency <= (select r_thr1 from thr)), 2,
            1) as R,

    frequency,
    multiIf((frequency <= (select f_thr0 from thr)), 1,
            (frequency <= (select f_thr1 from thr)), 2,
            3) as F,
    
    monetary,
    multiIf((monetary <= (select m_thr0 from thr)), 1,
            (monetary <= (select m_thr1 from thr)), 2,
            3) as M,
    
    concat(R::String, F::String, M::String) as RFM,
    
    multiIf(RFM in ('333', '323'), 'Best customers', 
            RFM in ('322', '332'), 'Promising',
            RFM in ('311', '312', '313'), 'New customers',
            RFM in ('233', '223', '213', '222', '212', '232'), 'Needs attention',
            RFM in ('133', '123', '113', '122', '132', '112'), 'At risk',
            RFM in ('331', '231', '321', '221', '311', '211'), 'Price sensitive',
            RFM in ('111', '121'), 'At risk price sensitive',
            'other') as segment

from users
