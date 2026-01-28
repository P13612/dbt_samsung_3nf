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
    MD5(feat.value:title::STRING || p.product_id) as feature_id,
    p.product_id,
    feat.value:title::STRING as feature_name,
    feat.value:description::STRING as feature_value
FROM parsed p,
LATERAL FLATTEN(input => p.json:productFeatureComponents) feat
WHERE feat.value:title::STRING IS NOT NULL
