select 
    player_id
    ,full_name
    ,player_url
    ,case
        when person_gender = 'M' then 'Male'
        when person_gender = 'F' then 'Female'
        else null
        end as person_gender
    ,case
        when POSITION('NE' in class_name) > 0 and ENDSWITH(LOWER(class_name),', ne') then TRIM(REPLACE(class_name,', NE'))
        when POSITION('NE' in class_name) > 0 then TRIM(REPLACE(class_name,'NE',''))
        else class_name
        end as class_name
    ,case
        when POSITION('NE' in class_name) > 0 then false
        else true
        end as is_eligible
    ,COALESCE(class_status, 'Unknown') as class_status
    ,person_npc_short
    ,age_in_yrs
    ,COALESCE(residence,'Unknown') as residence
    ,COALESCE(club,'Unknown')as club
    ,COALESCE(impairment,'Unknown') as impairment
    ,COALESCE(profession,'Unknown') as profession
    ,TRIM(REGEXP_REPLACE(first_tournament, '\\d{4}', '', 1)) as first_competition_name
    ,CAST(REGEXP_SUBSTR(first_tournament, '\\d{4}', 1) as number) as first_competition_year
    ,TRIM(REGEXP_REPLACE(last_tournament, '\\d{4}', '', 1)) as last_competition_name
    ,CAST(REGEXP_SUBSTR(last_tournament, '\\d{4}', 1) as number) as last_competition_year
    -- ,best_rank
    ,IFF(best_rank is not null, REPLACE(SPLIT(SPLIT(best_rank,' was on ')[0],'in')[0],'#',''), null) as bst_rnk_num
    ,TRIM(REPLACE(SPLIT(SPLIT(best_rank,' was on ')[0],'in')[1],'class','')) as bst_rnk_class_num
    ,INITCAP(SPLIT(SPLIT(best_rank,' was on ')[1],' ')[0]::string) as bst_rnk_month
    ,SPLIT(SPLIT(best_rank,' was on ')[1],' ')[1]::number as bst_rnk_year
    -- ,current_rank
    ,case
        when current_rank is null then null
        when POSITION('# ' in current_rank) > 0 then null
        when POSITION('was on' in current_rank) > 0 then TRY_TO_NUMBER(REPLACE(SPLIT(SUBSTRING(current_rank,2),'was on')[0],'\n\n\n view history',''))
        when POSITION('and' in current_rank) > 0 then TRY_TO_NUMBER(REPLACE(SPLIT(SPLIT(SUBSTRING(current_rank,2),' and ')[0],' in class ')[0],'\n\n\n view history',''))
        else TRY_TO_NUMBER(TRIM(SPLIT(SUBSTRING(current_rank,2),' ')[0]))
        end as curr_rnk_num
    ,case
        when current_rank is null then null
        when POSITION(' class ' in current_rank) = 0 then null
        else TRIM(SPLIT(SPLIT(SPLIT(current_rank,' and ')[0],' was on ')[0],'in class ')[1])
        end as curr_rnk_class_num
    ,case 
        when 
            bst_rnk_num = curr_rnk_num 
            and bst_rnk_class_num = curr_rnk_class_num 
            and bst_rnk_num is not null 
            and curr_rnk_num is not null 
            and bst_rnk_class_num is not null 
            and curr_rnk_class_num is not null 
            then true 
        when  
            bst_rnk_num is null 
            and curr_rnk_num is null 
            and bst_rnk_class_num is null 
            and curr_rnk_class_num is null  
            then null
        else false
        end as is_curr_rnk_best
from
    {{ref('r_player_profiles')}}
order by
    player_id asc