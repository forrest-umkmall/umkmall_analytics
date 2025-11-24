{% macro normalize_phone_number(column_name) %}
case
    when {{ column_name }} is null or trim({{ column_name }}) = '' then null
    else
        case
            when length(
                case
                    -- Remove +62 prefix
                    when regexp_replace({{ column_name }}, '[^0-9+]', '', 'g') like '+62%'
                    then substring(regexp_replace({{ column_name }}, '[^0-9]', '', 'g') from 3)
                    -- Remove 62 prefix
                    when regexp_replace({{ column_name }}, '[^0-9]', '', 'g') like '62%'
                    then substring(regexp_replace({{ column_name }}, '[^0-9]', '', 'g') from 3)
                    -- Remove leading 0
                    when regexp_replace({{ column_name }}, '[^0-9]', '', 'g') like '0%'
                    then substring(regexp_replace({{ column_name }}, '[^0-9]', '', 'g') from 2)
                    -- Already clean
                    else regexp_replace({{ column_name }}, '[^0-9]', '', 'g')
                end
            ) >= 9 then '+62' ||
                case
                    -- Remove +62 prefix
                    when regexp_replace({{ column_name }}, '[^0-9+]', '', 'g') like '+62%'
                    then substring(regexp_replace({{ column_name }}, '[^0-9]', '', 'g') from 3)
                    -- Remove 62 prefix
                    when regexp_replace({{ column_name }}, '[^0-9]', '', 'g') like '62%'
                    then substring(regexp_replace({{ column_name }}, '[^0-9]', '', 'g') from 3)
                    -- Remove leading 0
                    when regexp_replace({{ column_name }}, '[^0-9]', '', 'g') like '0%'
                    then substring(regexp_replace({{ column_name }}, '[^0-9]', '', 'g') from 2)
                    -- Already clean
                    else regexp_replace({{ column_name }}, '[^0-9]', '', 'g')
                end
            else null
        end
end
{% endmacro %}
