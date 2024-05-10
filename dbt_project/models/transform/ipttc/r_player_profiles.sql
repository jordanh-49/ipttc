select 
    sp.player_id
    ,sp.name as full_name
    ,sp.player_url
    ,sp.gender as person_gender
    ,sp.class_name
    ,ep.class_status
    ,sp.noc_short as person_npc_short
    ,ep.age as age_in_yrs
    ,COALESCE(ep.first_tournament,'Unknown') as first_tournament
    ,COALESCE(ep.last_tournament,'Unknown') as last_tournament
    ,ep.current_rank
    ,ep.best_rank
    ,ep.residence
    ,ep.club
    ,ep.impairment
    ,ep.profession
from
    {{source('ipttc', '_python_raw_profile_short')}} sp
    left join {{source('ipttc', '_python_raw_profile_extended')}} ep
    on sp.player_id = ep.player_id