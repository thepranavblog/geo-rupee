{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}

select
    event_id,
    event_timestamp,
    actor1_country as country_key,
    cameo_root_code as event_type_key,
    goldstein_score,
    case
        when goldstein_score between -10 and -7 then '[-10,-7]'
        when goldstein_score between -7  and -4 then '[-7,-4]'
        when goldstein_score between -4  and  0 then '[-4,0]'
        when goldstein_score between  0  and  4 then '[0,4]'
        when goldstein_score between  4  and  7 then '[4,7]'
        when goldstein_score between  7  and 10 then '[7,10]'
    end as goldstein_band,
    num_articles,
    avg_tone,
    rupee_at_event,
    change_15min_pct,
    change_1hr_pct,
    change_2hr_pct
from {{ ref('int_correlated_events') }}

{% if is_incremental() %}
where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}
