select distinct
    actor1_country as country_key,
    actor1_country as country_code,
    case actor1_country
        when 'IND' then 'India'
        when 'CHN' then 'China'
        when 'USA' then 'United States'
        when 'RUS' then 'Russia'
        when 'UAE' then 'United Arab Emirates'
        when 'SAU' then 'Saudi Arabia'
        when 'PAK' then 'Pakistan'
        when 'GBR' then 'United Kingdom'
        when 'JPN' then 'Japan'
        when 'DEU' then 'Germany'
        else actor1_country
    end as country_name,
    case
        when actor1_country in ('IND', 'PAK', 'BGD', 'LKA') then 'South Asia'
        when actor1_country in ('CHN', 'JPN', 'KOR')         then 'East Asia'
        when actor1_country in ('UAE', 'SAU', 'IRN', 'IRQ') then 'Middle East'
        when actor1_country in ('USA', 'GBR', 'DEU', 'FRA') then 'Western'
        else 'Other'
    end as region,
    case
        when actor1_country in ('USA', 'CHN', 'UAE', 'SAU', 'RUS', 'JPN', 'GBR', 'DEU')
        then true else false
    end as is_india_trade_partner
from {{ source('s3_bronze', 'gdelt_raw') }}
