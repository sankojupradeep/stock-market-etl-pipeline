-- Use the `ref` function to select from other models

select *
from STOCK_MARKET.RAW_DATA.my_first_dbt_model
where id = 1