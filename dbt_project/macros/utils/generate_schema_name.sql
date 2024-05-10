{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        This has been modified to always return the base schema
        and will never prefix a schema with the one from the
        profile file regardless of environment
    #}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}
    {%- else -%}

       {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}