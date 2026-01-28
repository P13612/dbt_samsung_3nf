WITH source AS (
    SELECT * FROM {{ source('raw', 'ECOM_SALES_REPORT_DE') }}
),

cleaned AS (
    SELECT
        -- 1. Generate Functional IDs (MD5 Hashes)
        MD5(COALESCE(ORDER_CODE,'') || '-' || COALESCE(PRODUCT_CODE,'') || '-' || COALESCE(ORDER_ENTRY_ID, '0')) AS sales_pk,
        
        MD5(UPPER(TRIM(STORE_NAME)))          AS store_id,
        MD5(UPPER(TRIM(ORDER_STATUS)))        AS status_id,
        MD5(UPPER(TRIM(CUSTOMER_TIER)))       AS tier_id,
        MD5(UPPER(TRIM(SALES_APPLICATION)))   AS app_id,
        MD5(UPPER(TRIM(CANCELLATION_REASON))) AS reason_id,
        MD5(UPPER(TRIM(GERP_SPROCID)))        AS proc_id,
        MD5(UPPER(TRIM(PRODUCT_CODE)))        AS product_id,
        MD5(UPPER(TRIM(PROMOTION_ID)))        AS promotion_id,

        -- 2. Clean Business Keys
        TRIM(ORDER_CODE) AS order_code,
        TRIM(PRODUCT_CODE) AS product_code,
        
        -- Store Attributes (for extraction)
        TRIM(STORE_NAME) AS store_name,
        TRIM(STORE_TYPE) AS store_type,
        TRIM(O2O_FLAG) AS o2o_flag,
        TRIM(O2O_STORE_TYPE) AS o2o_store_type,

        -- Dimension Attributes (for extraction)
        TRIM(ORDER_STATUS) AS status_name,
        TRIM(CUSTOMER_TIER) AS tier_name,
        TRIM(SALES_APPLICATION) AS app_name,
        TRIM(CANCELLATION_REASON) AS reason_text,
        TRIM(GERP_SPROCID) AS proc_name,

        -- 3. Metrics & Dates
        TRY_TO_DATE(ORDER_CREATION_DATE) AS order_date,
        TRY_CAST(ORDER_ENTRY_QUANTITY AS NUMBER(38,0)) AS quantity,
        TRY_CAST(ORDER_ENTRY_TOTAL_PRICE AS DECIMAL(18,2)) AS amount,
        'EUR' AS currency

    FROM source
)

SELECT * FROM cleaned
