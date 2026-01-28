
with source as (

    select * from {{ source('raw', 'D2C_MAPPING_DE') }}

),

renamed as (

    select
        "SKU" as sku,
        "BU" as business_unit,
        "SUB_BU" as sub_business_unit,
        "CATEGORY_GROUP" as category_group,
        "CATEGORY_TYPE" as category_type,
        "CONNECTIVITY" as connectivity,
        "LAUNCH_YEAR" as launch_year,
        "SEGMENT" as segment

    from source

)

select * from renamed
