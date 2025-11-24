{% macro normalize_employee_count(column_name) %}
case {{ column_name }}
    -- 0 employees
    when '0' then '0 karyawan'

    -- 1-5 employees
    when '1 - 5 orang' then '1-5 karyawan'
    when '1-3_karyawan' then '1-5 karyawan'
    when '3-5_karyawan' then '1-5 karyawan'
    when '1-3 Karyawan' then '1-5 karyawan'
    when '3-5 Karyawan' then '1-5 karyawan'

    -- 6-10 employees
    when '6 - 10 orang' then '6-10 karyawan'
    when '5-10_karyawan' then '6-10 karyawan'
    when '5-10 Karyawan' then '6-10 karyawan'

    -- 11-20 employees
    when '11 - 20 orang' then '11-20 karyawan'

    -- 21-50 employees
    when '21 - 50 orang' then '21-50 karyawan'

    -- 50+ employees
    when '> 50 orang' then '50+ karyawan'

    -- Map lebih_dari_10 to null (ambiguous)
    when 'lebih_dari_10' then null

    else {{ column_name }}
end
{% endmacro %}
