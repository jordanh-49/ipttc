select
    *
    exclude (splits_home,members_home,splits_away,members_away)
from
    {{ref('t_match_info')}}