{% macro drop_target_databases() %}
    {%- if target.name in ('prod') -%}
        {% do log("We don't drop production databases in scripts", info=True) %}
    {%- else -%}
        {% set sql %}
            DROP DATABASE IF EXISTS {{ target.database }}_transform;
            DROP DATABASE IF EXISTS {{ target.database }}_production;
        {% endset %}

        {% do run_query(sql) %}

        {% do log("Temporary DB's dropped", info=True) %}
    {%- endif -%}
{% endmacro %}