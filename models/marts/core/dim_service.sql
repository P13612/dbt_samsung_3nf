{{ config(materialized='table') }}

SELECT 
    product_id as service_id,
    sku_code as service_code,
    product_name as service_name,
    list_price as fee_amount
FROM {{ ref('stg_pim_products') }}
WHERE item_type = 'SERVICE'
