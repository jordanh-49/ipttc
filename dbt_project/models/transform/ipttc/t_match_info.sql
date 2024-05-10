select
    m.match_id
    ,m.competition_id
    ,c.competition_name
    ,c.competition_city
    ,c.competition_country
    ,m.event_id
    ,m.phase_id
    ,m.match_date
    ,m.time
    ,m.rtime
    ,m.match_desc
    ,m.table_num
    ,m.table_desc
    ,m.venue
    ,m.status
    ,m.is_team
    ,m.has_comps
    ,m.has_stats
    ,m.team_id_home
    ,m.team_name_home
    ,m.team_noc_short_home
    ,m.team_noc_home
    ,m.sets_won_home
    ,m.is_walkover_home
    ,m.rkpo_home
    ,m.splits_home
    ,m.members_home
    ,m.team_id_away
    ,m.team_name_away
    ,m.team_noc_short_away
    ,m.team_noc_away
    ,m.sets_won_away
    ,m.is_walkover_away
    ,m.rkpo_away
    ,m.splits_away
    ,m.members_away
    ,m.is_draw
    ,m.victory_team_id
from
    {{ref('r_match_info')}} m
    left join {{ref('r_competition_info')}} c
    on m.competition_id = c.competition_id
order by
    m.competition_id
    ,m.event_id
    ,m.phase_id
    ,m.match_date ASC
    ,m.match_id