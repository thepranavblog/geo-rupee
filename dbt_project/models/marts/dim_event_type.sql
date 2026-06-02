select distinct
    cameo_root_code as event_type_key,
    cameo_code,
    cameo_root_code,
    case cameo_root_code
        when '01' then 'Make Public Statement'
        when '02' then 'Appeal'
        when '03' then 'Express Intent to Cooperate'
        when '04' then 'Consult'
        when '05' then 'Diplomatic Cooperation'
        when '06' then 'Material Cooperation'
        when '07' then 'Provide Aid'
        when '08' then 'Yield'
        when '09' then 'Investigate'
        when '10' then 'Demand'
        when '11' then 'Disapprove'
        when '12' then 'Reject'
        when '13' then 'Threaten'
        when '14' then 'Protest'
        when '15' then 'Exhibit Force'
        when '16' then 'Reduce Relations'
        when '17' then 'Coerce'
        when '18' then 'Assault'
        when '19' then 'Fight'
        when '20' then 'Use Unconventional Mass Violence'
        else 'Unknown'
    end as category_label,
    case
        when cameo_root_code in ('19', '20', '18', '16', '17') then 'High'
        when cameo_root_code in ('14', '15', '13', '11', '12') then 'Medium'
        else 'Low'
    end as typical_sensitivity
from {{ source('s3_bronze', 'gdelt_raw') }}
