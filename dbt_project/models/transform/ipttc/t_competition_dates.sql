select
    c.ipttc_competition_id
    ,c.competition_id
    ,c.competition_name
    ,c.is_finished
    ,try_to_date(fd.value:"raw"::string) as competition_date
    ,c.date_description
    ,fd.value:"forCal"::string as date_calendar
from
    {{ref('r_competition_event_info')}} c
    ,lateral flatten(input => c.dates) fd