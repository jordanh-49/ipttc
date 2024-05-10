select
    champ as ipttc_competition_id
    ,competitionid as competition_id
    ,champdesc as competition_name
    ,try_to_boolean(isfinished::string) as is_finished
    ,location as competition_location
    ,status
    ,dates
    ,events
    ,phases
    ,locations
    ,datesdesc as date_description
from
    {{source('ipttc', '_python_raw_event_info')}}