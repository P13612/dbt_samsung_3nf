{{ config(materialized='table') }}

SELECT * FROM {{ ref('stg_ecom_promotions') }}
