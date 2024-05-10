select
    r.match_id
    ,r.competition_id
    ,r.event_id
    ,r.phase_id
    ,r.team_id_home as team_id
    ,r.team_name_home as team_name
    ,r.team_noc_short_home as team_noc_short
    ,r.team_noc_home as team_noc
    ,r.sets_won_home as sets_won
    ,r.is_walkover_home as is_walkover
    ,r.is_draw
    ,r.victory_team_id
    ,r.rkpo_home as rkpo
    ,r.splits_home as splits
    ,r.members_home as members
from
    {{ref('t_match_info')}} r
            
union

select
    r.match_id
    ,r.competition_id
    ,r.event_id
    ,r.phase_id
    ,r.team_id_away as team_id
    ,r.team_name_away as team_name
    ,r.team_noc_short_away as team_noc_short
    ,r.team_noc_away as team_noc
    ,r.sets_won_away as sets_won
    ,r.is_walkover_away as is_walkover
    ,r.is_draw
    ,r.victory_team_id
    ,r.rkpo_away as rkpo
    ,r.splits_away as splits
    ,r.members_away as members
from
    {{ref('t_match_info')}} r
order by
    competition_id
    ,event_id
    ,phase_id
    ,match_id