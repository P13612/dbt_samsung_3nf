
with 
sales as (
    select * from {{ ref('stg_ecom_sales') }}
),

products as (
    select * from {{ ref('dim_product') }}
),

promotions as (
    select * from {{ ref('dim_promotion') }}
),

final as (
    select
        -- Measures
        sales.order_entry_quantity,
        sales.order_entry_total_price,
        sales.total_rrp,

        -- Foreign Keys
        sales.product_code as product_key,
        sales.promotion_name as promotion_key,
        sales.order_status as order_status_key,
        sales.store_id as store_key,
        sales.order_creation_date as date_key,

        -- Degenerate Dimensions
        sales.order_code,
        sales.voucher_code
        
    from sales
    left join products on sales.product_code = products.sku
    left join promotions on sales.promotion_name = promotions.promotion_name_extracted
)

select * from final
