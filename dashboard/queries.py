KPI_QUERY = """
select
    count(*) as total_events,
    round(avg(change_1hr_pct), 3) as avg_rupee_move,
    round(min(change_2hr_pct), 3) as biggest_drop,
    mode(country_key) as most_active_country
from fact_correlation
where event_timestamp >= current_date - interval '{days} days'
"""

CAMEO_QUERY = """
select
    e.category_label,
    round(avg(f.change_1hr_pct), 3) as avg_rupee_change,
    count(*) as event_count
from fact_correlation f
join dim_event_type e on f.event_type_key = e.event_type_key
where f.event_timestamp >= current_date - interval '{days} days'
group by e.category_label
order by avg_rupee_change
"""

COUNTRY_QUERY = """
select
    d.country_name,
    d.region,
    count(*) as event_count,
    round(avg(f.change_15min_pct), 3) as avg_15min,
    round(avg(f.change_1hr_pct), 3) as avg_1hr,
    round(avg(f.change_2hr_pct), 3) as avg_2hr
from fact_correlation f
join dim_country d on f.country_key = d.country_key
where f.event_timestamp >= current_date - interval '{days} days'
group by d.country_name, d.region
order by avg_1hr
"""

SCATTER_QUERY = """
select
    f.goldstein_score,
    f.change_1hr_pct,
    e.category_label,
    d.country_name,
    f.event_timestamp
from fact_correlation f
join dim_event_type e on f.event_type_key = e.event_type_key
join dim_country d on f.country_key = d.country_key
where f.event_timestamp >= current_date - interval '{days} days'
"""

TIMELINE_QUERY = """
select
    date_trunc('day', f.event_timestamp) as event_date,
    round(avg(f.rupee_at_event), 2) as avg_rupee_rate,
    round(min(f.goldstein_score), 1) as min_goldstein,
    count(*) as event_count
from fact_correlation f
where f.event_timestamp >= current_date - interval '{days} days'
group by event_date
order by event_date
"""
