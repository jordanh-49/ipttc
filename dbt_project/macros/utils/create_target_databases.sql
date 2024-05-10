{% macro create_target_databases() %}
    {#
        Tries to create a copy of all databases needed using the
        specified target. Only needed if you are working with a new target
        or have for some reason dropped databases (or new ones have been added)

        This is not run automatically, it would be run using run-operation:

           dbt run-operation create_target_databases --target <target name>

    #}
    {% do log(target.name, info=True) %}

    {%- if target.name in ('prod') -%}
        {% set sql %}
        CREATE DATABASE IF NOT EXISTS TRANSFORM;
        CREATE DATABASE IF NOT EXISTS PRODUCTION;
        {% endset %}

        {% do log("Creating databases for Production environment", info=True) %}
    {%- else -%}
        {% set sql %}
        CREATE DATABASE IF NOT EXISTS {{target.database}}_TRANSFORM;
        CREATE DATABASE IF NOT EXISTS {{target.database}}_PRODUCTION;
        {% endset %}

        {% do log("Creating databases for Non-production environment", info=True) %}
    {%- endif -%}

    {% do run_query(sql) %}
{% endmacro %}