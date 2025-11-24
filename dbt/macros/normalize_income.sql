{% macro normalize_income(column_name) %}
case {{ column_name }}
    -- Below 10 million
    when 'Dibawah 10 juta' then 'dibawah 10 juta'
    when 'dibawah_10_juta' then 'dibawah 10 juta'

    -- 10-50 million
    when '10 - 30 juta' then '10-50 juta'
    when '10_-_50_juta' then '10-50 juta'
    when '31 - 50 juta' then '10-50 juta'
    when '10 - 50 juta' then '10-50 juta'

    -- 50-100 million
    when '50_-_100_juta' then '50-100 juta'
    when '51 - 70 juta' then '50-100 juta'
    when '71 - 100 juta' then '50-100 juta'

    -- Above 100 million
    when 'Diatas 100 juta' then 'diatas 100 juta'
    when '100_-_200_juta' then 'diatas 100 juta'
    when 'diatas_200_juta' then 'diatas 100 juta'

    else {{ column_name }}
end
{% endmacro %}
