{% test expect_column_values_to_match_regex(model, column_name,
                                                    regex,
                                                    row_condition=None,
                                                    is_raw=False
                                                    ) %}

{% set expression %}
{{ dbt_expectations.regexp_instr(column_name, regex, is_raw=is_raw) }} > 0
{% endset %}

{{ dbt_expectations.expression_is_true(model,
                                        expression=expression,
                                        group_by_columns=None,
                                        row_condition=row_condition
                                        )
                                        }}

{% endtest %}