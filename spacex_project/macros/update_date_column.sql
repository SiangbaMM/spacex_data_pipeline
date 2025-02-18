{#
    Macro to update date/timestamp columns in a table with support for Type 2 SCD.

    Args:
        table_name (ref): The table to update
        column_name (str): The name of the date column to update
        date_value (str, optional): Specific date/timestamp value. Defaults to current timestamp
        scd_type_2 (bool, optional): Whether to handle Type 2 SCD updates. Defaults to false
        valid_from_column (str, optional): Column name for SCD valid_from. Required if scd_type_2 is true
        valid_to_column (str, optional): Column name for SCD valid_to. Required if scd_type_2 is true
        is_current_column (str, optional): Column name for SCD is_current flag. Required if scd_type_2 is true

    Example:
        -- Basic usage (update last_modified)
        {{ config(
            post_hook="{{ update_date_column(this, 'last_modified_at') }}"
        ) }}

        -- Type 2 SCD usage
        {{ config(
            post_hook="{{ update_date_column(
                this,
                'valid_from',
                scd_type_2=true,
                valid_from_column='valid_from',
                valid_to_column='valid_to',
                is_current_column='is_current'
            ) }}"
        ) }}

        -- Custom date value
        {{ config(
            post_hook="{{ update_date_column(
                this,
                'created_at',
                date_value='2024-01-01'::timestamp
            ) }}"
        ) }}
#}

{% macro update_date_column(
    table_name,
    column_name,
    date_value=none,
    scd_type_2=false,
    valid_from_column=none,
    valid_to_column=none,
    is_current_column=none
) %}

{# Input validation #}
{% if scd_type_2 and (valid_from_column is none or valid_to_column is none or is_current_column is none) %}
    {{ exceptions.raise_compiler_error("SCD Type 2 requires valid_from_column, valid_to_column, and is_current_column") }}
{% endif %}

{# Set default date value #}
{%- if date_value is none %}
    {%- set date_value = "CURRENT_TIMESTAMP()" %}
{%- endif %}

{% set query %}
    {% if scd_type_2 %}
        -- Type 2 SCD update
        UPDATE {{ table_name }}
        SET
            {{ valid_to_column }} = {{ date_value }},
            {{ is_current_column }} = FALSE
        WHERE
            {{ valid_to_column }} IS NULL
            AND {{ is_current_column }} = TRUE;

        UPDATE {{ table_name }}
        SET
            {{ valid_from_column }} = {{ date_value }},
            {{ is_current_column }} = TRUE
        WHERE
            {{ valid_from_column }} IS NULL;
    {% else %}
        -- Simple date column update
        UPDATE {{ table_name }}
        SET {{ column_name }} = {{ date_value }}
        WHERE {{ column_name }} IS NULL;
    {% endif %}
{% endset %}

{% do run_query(query) %}

{# Log the operation #}
{{ log("Updated date column '" ~ column_name ~ "' in table " ~ table_name, info=true) }}

{% endmacro %}
