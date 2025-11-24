{% macro normalize_email(column_name) %}
case
    when {{ column_name }} is not null and {{ column_name }} != ''
         and {{ column_name }} like '%@%.%'
    then lower(trim({{ column_name }}))
    else null
end
{% endmacro %}
