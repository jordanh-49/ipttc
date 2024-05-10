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
            ,s.index + 1 as set_number
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
    *
from 
    match_sets
where 
    set_point is not null
group by all
order by
    competition_id
    ,event_id
    ,phase_id
    ,match_id
    ,rkpo asc