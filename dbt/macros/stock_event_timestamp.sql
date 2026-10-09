{% macro stock_event_timestamp(expression) -%}
    {{ return(
        adapter.dispatch(
            'stock_event_timestamp',
            'stock_analytics'
        )(expression)
    ) }}
{%- endmacro %}


{% macro athena__stock_event_timestamp(expression) -%}
    cast(
        at_timezone(
            try(from_iso8601_timestamp({{ expression }})),
            'UTC'
        ) as timestamp
    )
{%- endmacro %}


{% macro snowflake__stock_event_timestamp(expression) -%}
    cast(
        convert_timezone(
            'UTC',
            try_to_timestamp_tz({{ expression }})
        ) as timestamp_ntz
    )
{%- endmacro %}


{% macro databricks__stock_event_timestamp(expression) -%}
    try_to_timestamp({{ expression }})
{%- endmacro %}


{% macro default__stock_event_timestamp(expression) -%}
    {{ exceptions.raise_compiler_error(
        "stock_event_timestamp is not implemented for adapter: "
        ~ target.type
    ) }}
{%- endmacro %}
