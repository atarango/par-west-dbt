{% macro ga4_param_string(params, key) -%}
( select ep.value.string_value
  from unnest({{ params }}) ep
  where ep.key = '{{ key }}'
  limit 1 )
{%- endmacro %}

{% macro ga4_param_int(params, key) -%}
( select coalesce(ep.value.int_value, cast(ep.value.string_value as int64))
  from unnest({{ params }}) ep
  where ep.key = '{{ key }}'
  limit 1 )
{%- endmacro %}

{% macro ga4_param_float(params, key) -%}
( select coalesce(ep.value.double_value,
                  cast(ep.value.int_value as float64),
                  cast(ep.value.string_value as float64))
  from unnest({{ params }}) ep
  where ep.key = '{{ key }}'
  limit 1 )
{%- endmacro %}
