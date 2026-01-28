with 
sales as (
    select distinct
        "ORDER_STATUS" as order_status,
        "CONSIGNMENT_STATUS" as consignment_status
    from {{ ref('stg_ecom_sales') }}
    where "ORDER_STATUS" is not null
)

select 
    {{ dbt_utils.generate_surrogate_key(['order_status', 'consignment_status']) }} as order_status_key,
    order_status,
    consignment_status
from sales