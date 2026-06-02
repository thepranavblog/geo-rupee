select distinct
    event_id,
    event_timestamp,
    actor1_country,
    actor2_country,
    cameo_code,
    cameo_root_code,
    goldstein_score,
    num_articles,
    avg_tone
from {{ source('s3_silver', 'correlated') }}
