with fiscal_date as (

    {{ dbt_date.get_fiscal_periods(ref('dim_date'), year_end_month=1, week_start_day=1) }}
)

select
    f.*
from
    fiscal_date f