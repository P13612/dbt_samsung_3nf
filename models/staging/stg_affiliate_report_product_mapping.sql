with source as (

    select * from {{ source('raw', 'AFFILIATE_REPORT_PRODUCT_MAPPING') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed