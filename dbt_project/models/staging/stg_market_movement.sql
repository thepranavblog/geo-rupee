select distinct
    event_id,
    rupee_at_event,
    change_15min_pct,
    change_1hr_pct,
    change_2hr_pct
from {{ source('s3_silver', 'correlated') }}
where rupee_at_event is not null
