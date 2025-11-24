{% macro normalize_business_age(column_name) %}
case {{ column_name }}
    -- Below 1 year
    when 'dibawah_1_tahun' then 'dibawah 1 tahun'
    when '< 1 tahun' then 'dibawah 1 tahun'

    -- 1-2 years
    when '1-2_tahun' then '1-2 tahun'
    when '1 - 2 tahun' then '1-2 tahun'

    -- 3-5 years
    when '3-5_tahun' then '3-5 tahun'
    when '3 - 5 tahun' then '3-5 tahun'

    -- 5-10 years
    when '6-10_tahun' then '5-10 tahun'
    when 'diatas_10_tahun' then '5-10 tahun'
    when '5 - 10 tahun' then '5-10 tahun'

    -- Above 10 years
    when 'diatas_20_tahun' then 'diatas 10 tahun'
    when '> 10 tahun' then 'diatas 10 tahun'

    else {{ column_name }}
end
{% endmacro %}
