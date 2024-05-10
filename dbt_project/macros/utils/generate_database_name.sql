{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {#
        Modelled on https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/utils/generate_warehouse_name.sql

        Definitions:
            - custom_database_name: database provided via dbt_project.yml or model config
            - target.name: name of the target (dev for local development, prod for production, etc.)
            - target.database: database provided by the target defined in profiles.yml
        
        Assumptions:
            - dbt users will have USERNAME_PROD, USERNAME_PREP DBs defined

        This macro is hard to test, but here are some test cases and expected output.
        (custom_database_name, target.name, target.database) = <output>

        (prod, prod, prep) = prod
        (prod, ci, prep) = prod
        (prod, dev, tony) = tony_prod
        
        (prep, prod, prep) = prep
        (prep, ci, prep) = prep
        (prep, dev, tony) = tony_prep
    #}

    {%- if target.name in ('prod') -%}

        {%- if custom_database_name is none -%}

            {{ target.database | trim }}
        {%- else -%}

            {{ custom_database_name | trim }}
        {%- endif -%}

    {%- else -%}

        {%- if custom_database_name is none -%}

            {# Should probably never happen.. #}
            {{ target.database | trim }}
        {%- else -%}

            {{ target.database }}_{{ custom_database_name | trim }}
        {%- endif -%}

    {%- endif -%}

{%- endmacro %}
