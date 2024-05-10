with 
    matches_by_player as (
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
            ,m.splits
            ,COALESCE(p.value:"Reg"::string, m.team_id) as player_id
            ,COALESCE(p.value:"Desc"::string, m.team_name) as player_name
            ,COALESCE(p.value:"Org"::string, m.team_noc_short) as player_org
        from
            {{ref('t_matches_by_team')}} m
            left join lateral flatten(input => m.members, outer => TRUE) p
    ),
    matches_by_players_by_set as (
        select
            t.match_id
            ,t.competition_id
            ,t.event_id
            ,t.phase_id
            ,t.team_id
            ,t.team_name
            ,t.team_noc_short
            ,t.team_noc
            ,t.sets_won
            ,t.is_walkover
            ,t.is_draw
            ,t.victory_team_id
            ,t.rkpo
            ,case
                when REGEXP_REPLACE(t.player_id, '\\D', '') = '' then null
                else CAST(REGEXP_REPLACE(t.player_id, '\\D', '') as number) 
                end as player_id
            ,t.player_name
            ,t.player_org
            ,s.index
            ,case
                when TRIM(s.value:"Res"::string) = '' then null
                else s.value:"Res"::integer
                end as set_point
            ,s.value:"Win"::boolean as is_set_win
        from 
        matches_by_player t
        ,lateral flatten(input => t.splits) s
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
    ,player_id
    ,player_name
    ,player_org
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
    matches_by_players_by_set
group by all
order by
    competition_id
    ,event_id
    ,phase_id
    ,match_id
    ,team_id
    ,rkpo asc