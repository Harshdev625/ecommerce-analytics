{% macro normalize_city(column_name) %}
    initcap(trim({{ column_name }}))
{% endmacro %}

{% macro normalize_state(column_name) %}
    upper(trim({{ column_name }}))
{% endmacro %}
