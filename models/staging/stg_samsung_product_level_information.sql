
with source as (

    select * from {{ source('raw', 'DIM_SAMSUNG_PRODUCT_LEVEL_INFORMATION') }}

),

renamed as (

    select
        "SKU_CODE" as sku,
        "PRODUCT_NAME" as product_name,
        "PRODUCT_TYPE" as product_type,
        "PRODUCT_SUB_TYPE" as product_sub_type,
        "PRODUCT_FAMILY" as product_family

    from source

)

select * from renamed
