select 
    competition_id
    ,competition_name
    ,competition_url
    ,city as competition_city
    ,country as competition_country
    ,TRY_TO_DATE(start_date) as start_date
from
    {{source('ipttc', '_python_raw_competitions')}}