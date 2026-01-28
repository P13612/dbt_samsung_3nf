{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ ref('stg_pim_products') }}
    WHERE item_type = 'HARDWARE'
),
parsed AS (
    SELECT
        product_id,
        {{ target.schema }}.parse_python_obj(product_blob) as json
    FROM source
)

SELECT
    MD5(img.value:url::STRING) as image_id,
    p.product_id,
    img.value:imageType::STRING as image_type,
    img.value:format::STRING as format,
    img.value:url::STRING as image_url,
    img.value:altText::STRING as alt_text
FROM parsed p,
LATERAL FLATTEN(input => p.json:images) img
QUALIFY ROW_NUMBER() OVER (PARTITION BY image_id ORDER BY product_id) = 1
