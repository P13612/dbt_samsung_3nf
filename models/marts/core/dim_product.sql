with 
product_info as (
    select * from {{ ref('stg_samsung_product_level_information') }}
),

d2c_mapping as (
    select * from {{ ref('stg_d2c_mapping') }}
),

affiliate_mapping as (
    select * from {{ ref('stg_affiliate_report_product_mapping') }}
),

shop_info as (
    select 
        "SKU_CODE" as sku,
        "PRODUCT_INFORMATION" as product_information_json
    from {{ source('raw', 'SAMSUNG_SHOP_PRODUCT_INFORMATION_DE') }}
),

final as (
    select 
        pi.sku,
        pi.product_name,
        pi.product_type,
        pi.product_sub_type,
        pi.product_family,
        dm.business_unit,
        dm.sub_business_unit,
        dm.category_group,
        dm.category_type,
        dm.connectivity,
        dm.launch_year,
        dm.segment,
        am.product_series,
        am.product_category,
        si.product_information_json
    from product_info pi
    left join d2c_mapping dm on pi.sku = dm.sku
    left join affiliate_mapping am on pi.sku = am.sku
    left join shop_info si on pi.sku = si.sku
)

select * from final