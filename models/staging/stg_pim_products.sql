-- Pre-hook ensures UDF exists
{{ config(
    pre_hook="{{ create_udf_parse_python_obj() }}"
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'SAMSUNG_SHOP_PRODUCT_INFORMATION_DE') }}
),

parsed AS (
    SELECT
        SKU_CODE as raw_sku_key,
        -- Use Python UDF to unlock the text blob
        {{ target.schema }}.parse_python_obj(PRODUCT_INFORMATION) as json_data,
        PRODUCT_INFORMATION as product_blob -- Keep raw for child tables
    FROM source
    WHERE SKU_CODE IS NOT NULL
),

extracted AS (
    SELECT
        -- IDs
        MD5(UPPER(TRIM(raw_sku_key))) AS product_id,
        TRIM(raw_sku_key) AS sku_code,
        product_blob,

        -- Attributes from JSON
        TRIM(json_data:name::STRING) AS product_name,
        TRIM(json_data:ean::STRING)  AS ean_code,
        TRIM(json_data:baseProduct::STRING) AS base_product_code,
        TRIM(json_data:description::STRING) AS description,
        
        -- Commercials
        TRY_CAST(json_data:price:value::STRING AS DECIMAL(10,2)) AS price_value,
        TRIM(json_data:price:currencyIso::STRING) AS currency,
        TRIM(json_data:price:priceType::STRING) AS price_type,
        
        -- Inventory
        TRY_CAST(json_data:stock:stockLevel::STRING AS NUMBER(38,0)) AS stock_level,
        TRIM(json_data:stock:stockLevelStatus::STRING) AS stock_status,
        
        -- Classification
        TRIM(json_data:url::STRING) as url_path,
        json_data:purchasable::BOOLEAN as is_purchasable,
        
        -- The Filter Logic (Family vs Product)
        json_data:familyId AS _family_id_check

    FROM parsed
    WHERE json_data IS NOT NULL
),

classified AS (
    SELECT 
        *,
        -- The "Semantic Splitter" Logic
        CASE 
            -- Tech/Hierarchy check
            WHEN _family_id_check IS NOT NULL THEN 'HIERARCHY'
            
            -- Service check
            WHEN sku_code LIKE 'SMC-%' OR sku_code LIKE 'CY-%' THEN 'SERVICE'
            WHEN UPPER(product_name) LIKE '%CARE+%' THEN 'SERVICE'
            WHEN UPPER(product_name) LIKE '%GARANTIE%' THEN 'SERVICE'
            
            -- Legal check
            WHEN UPPER(product_name) LIKE '%ZINSSATZ%' THEN 'LEGAL'
            
            -- Default
            ELSE 'HARDWARE'
        END AS item_type
    FROM extracted
)

-- Final Deduplication (1 Row per SKU)
SELECT
    product_id,
    sku_code,
    item_type,
    product_blob, -- Needed for child tables
    MAX(product_name) as product_name,
    MAX(ean_code) as ean_code,
    MAX(base_product_code) as base_product_code,
    MAX(description) as description,
    MAX(price_value) as list_price,
    MAX(currency) as currency,
    MAX(price_type) as price_type,
    MAX(stock_level) as stock_level,
    MAX(stock_status) as stock_status,
    MAX(is_purchasable) as is_purchasable,
    -- Simple Category Extraction from URL
    MAX(SPLIT_PART(url_path, '/', 2)) as category_level_1,
    MAX(SPLIT_PART(url_path, '/', 3)) as category_level_2,
    MAX(url_path) as category_path
FROM classified
WHERE item_type IN ('HARDWARE', 'SERVICE') -- Drop Hierarchy and Legal
GROUP BY 1, 2, 3, 4
