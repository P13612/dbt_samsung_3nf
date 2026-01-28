
with source as (

    select * from {{ source('raw', 'AFFILIATE_REPORT_PRODUCT_MAPPING') }}

),

renamed as (

    select
        "SKU_SUBCODE" as sku,
        "PRODUCTS" as product,
        "PRODUCT_SERIES" as product_series,
        "PRODUCT_CATEGORY" as product_category

    from source

)

select * from renamed
