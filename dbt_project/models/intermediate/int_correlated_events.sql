select
    e.event_id,
    e.event_timestamp,
    e.actor1_country,
    e.actor2_country,
    e.cameo_code,
    e.cameo_root_code,
    e.goldstein_score,
    e.num_articles,
    e.avg_tone,
    m.rupee_at_event,
    m.change_15min_pct,
    m.change_1hr_pct,
    m.change_2hr_pct
from {{ ref('stg_geopolitical_events') }} e
left join {{ ref('stg_market_movement') }} m
    on e.event_id = m.event_id
