select
    c.ipttc_competition_id
    ,c.competition_id
    ,fp.value:"EvKey"::string as event_id
    ,fp.value:"Key"::string as phase_id
    ,fp.value:"Desc"::string as phase_name
    ,fp.value:"Type"::string as phase_type
from
    {{ref('r_competition_event_info')}} c
    ,lateral flatten(input => c.phases) fp