select
    key as match_id
    ,competitionid as competition_id
    ,ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(MATCH_ID, '.'), 0, 2), '.') as event_id
    ,ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(MATCH_ID, '.'), 0, 3), '.') as phase_id
    ,matchdate as match_date
    ,time
    ,rtime
    ,desc as match_desc
    ,loc as table_num
    ,locdesc as table_desc
    ,venue
    ,status
    ,isteam as is_team
    ,hascomps as has_comps
    ,hasstats as has_stats
    ,home:"Reg"::string as team_id_home
    ,home:"Desc"::string as team_name_home
    ,home:"Org"::string as team_noc_short_home
    ,home:"OrgDesc"::string as team_noc_home
    ,case 
        when home:"Res"::string = 'WO' then null
        else TRY_CAST(home:"Res"::string as integer)
        end as sets_won_home
    ,case when home:"Res"::string = 'WO' then true
        else false
        end as is_walkover_home
    ,home:"RkPo"::integer as rkpo_home
    ,home:"Splits" as splits_home
    ,home:"Members" as members_home
    ,away:"Reg"::string as team_id_away
    ,away:"Desc"::string as team_name_away
    ,away:"Org"::string as team_noc_short_away
    ,away:"OrgDesc"::string as team_noc_away
    ,case 
        when away:"Res"::string = 'WO' then null
        else TRY_CAST(away:"Res"::string as integer)
        end as sets_won_away
    ,case when away:"Res"::string = 'WO' then true
        else false
        end as is_walkover_away
    ,away:"RkPo"::integer as rkpo_away
    ,away:"Splits" as splits_away
    ,away:"Members" as members_away
    ,case when home:"Win"::boolean = FALSE and away:"Win"::boolean = FALSE then true else false end as is_draw
    ,case 
        when home:"Win"::boolean THEN team_id_home
        when away:"Win"::boolean THEN team_id_away
        else null
    end as victory_team_id
from
    {{source('ipttc', '_python_raw_results')}}