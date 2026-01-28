with 
sales as (
    select distinct
        "STORE_ID" as store_id,
        "STORE_NAME" as store_name
    from {{ ref('stg_ecom_sales') }}
    where "STORE_ID" is not null
)

select 
    store_id,
    store_name
from sales