with source as (

    select * from {{ source('raw', 'DIM_SAMSUNG_PRODUCT_LEVEL_INFORMATION') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed