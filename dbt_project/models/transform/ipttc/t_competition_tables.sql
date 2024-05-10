select
    c.ipttc_competition_id
    ,c.competition_id
    ,fl.value:"Key"::string as table_id
    ,fl.value:"Desc"::string as table_description
from
    {{ref('r_competition_event_info')}} c
    ,lateral flatten(input => c.locations) fl