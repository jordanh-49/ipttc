select
    c.ipttc_competition_id
    ,c.competition_id
    ,fe.value:"Key"::STRING AS event_id
    ,fe.value:"Desc"::STRING AS event_description
    ,CASE
        WHEN event_id LIKE 'M.%' THEN 'Men'
        WHEN event_id LIKE 'W.%' THEN 'Women'
        WHEN event_id LIKE 'X.%' THEN 'Mixed'
        ELSE NULL
    END AS event_gender
    ,CASE
        WHEN event_id LIKE 'M.SINGLES%' THEN 'Singles - Men'
        WHEN event_id LIKE 'M.DOUBLES%' THEN 'Doubles - Men'
        WHEN event_id LIKE 'W.SINGLES%' THEN 'Singles - Women'
        WHEN event_id LIKE 'W.DOUBLES%' THEN 'Doubles - Women'
        WHEN event_id LIKE 'X.DOUBLES%' THEN 'Mixed Doubles'
        ELSE NULL
    END AS event_name
    ,REGEXP_REPLACE(SUBSTRING(event_id, 16), '-+$', '') AS event_class
from
    {{ref('r_competition_event_info')}} c
    ,lateral flatten (input => c.events) AS fe