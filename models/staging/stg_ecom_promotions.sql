WITH source AS (
    SELECT * FROM {{ source('raw', 'DIM_ECOM_PROMOTIONS') }}
),

cleaned AS (
    SELECT
        MD5(UPPER(TRIM(PROMOTION_ID))) AS promotion_id,
        TRIM(PROMOTION_NAME) AS promotion_name,

        -- Semantic Renaming (OB5/OB6 -> Business Terms)
        TRIM(OB5_FL) AS benefit_header,
        TRIM(OB5_SL) AS benefit_sub_header,
        TRIM(OB6_HL) AS condition_header,
        TRIM(OB6_SL) AS condition_details

    FROM source
    WHERE PROMOTION_ID IS NOT NULL
)

SELECT * FROM cleaned
