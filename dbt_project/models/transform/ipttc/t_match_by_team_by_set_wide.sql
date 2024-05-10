with 
    match_sets AS (
      select 
        m.match_id
        ,m.competition_id
        ,m.event_id
        ,m.phase_id
        ,m.team_id
        ,m.team_name
        ,m.team_noc_short
        ,m.team_noc
        ,m.sets_won
        ,m.is_walkover
        ,m.is_draw
        ,m.victory_team_id
        ,m.rkpo
        ,s.index
        ,case
            when TRIM(s.value:"Res"::string) = '' then null
            else s.value:"Res"::integer
            end as set_point
        ,s.value:"Win"::boolean as is_set_win
      from 
        {{ref('t_matches_by_team')}} m
        ,lateral flatten(input => m.splits) s
    )
select 
    match_id
    ,competition_id
    ,event_id
    ,phase_id
    ,team_id
    ,team_name
    ,team_noc_short
    ,team_noc
    ,sets_won
    ,is_walkover
    ,is_draw
    ,victory_team_id
    ,rkpo
    ,MAX(case when index = 0 then is_set_win end) as is_set_1_win
    ,MAX(case when index = 0 then set_point end) as set_1_point 
    ,MAX(case when index = 1 then is_set_win end) as is_set_2_win
    ,MAX(case when index = 1 then set_point end) as set_2_point
    ,MAX(case when index = 2 then is_set_win end) as is_set_3_win
    ,MAX(case when index = 2 then set_point end) as set_3_point 
    ,MAX(case when index = 3 then is_set_win end) as is_set_4_win
    ,MAX(case when index = 3 then set_point end) as set_4_point 
    ,MAX(case when index = 4 then is_set_win end) as is_set_5_win
    ,MAX(case when index = 4 then set_point end) as set_5_point 
from 
    match_sets
group by all
order by
    competition_id
    ,event_id
    ,phase_id
    ,match_id
    ,rkpo asc