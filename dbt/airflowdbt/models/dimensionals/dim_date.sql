with dates as (

  {{ dbt_date.get_date_dimension('2023-01-01', '2023-12-31') }}

)

select
    d.date_day,
    d.day_of_week,
    d.day_of_week_name,
    d.day_of_week_name_short,
    d.day_of_month,
    d.day_of_year,
    d.week_start_date,
    d.week_end_date,
    d.week_of_year,
    d.month_of_year,
    d.month_name,
    d.month_name_short,
    d.month_end_date,
    d.quarter_of_year,
    d.quarter_start_date,
    d.quarter_end_date,
    d.year_number,
    d.year_start_date,
    d.year_end_date

from dates d