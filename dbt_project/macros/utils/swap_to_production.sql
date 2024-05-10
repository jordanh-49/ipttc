{% macro swap_to_production() %}

{% set sql %}
     ALTER DATABASE IF EXISTS master_transform SWAP WITH transform;
     ALTER DATABASE IF EXISTS master_production SWAP WITH production;
{% endset %}

{% do run_query(sql) %}
{% do log("production databases swapped", info=True) %}
{% endmacro %}