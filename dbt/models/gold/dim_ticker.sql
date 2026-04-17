{{
    config(
        materialized='table',
        unique_key='ticker_symbol'
    )
}}

/*
  Gold Layer: dim_ticker
  ----------------------
  Slowly Changing Dimension (Type 1) for stock ticker metadata.
  Loaded from the Silver layer's distinct ticker/company attributes.
*/

WITH source AS (
    SELECT DISTINCT
        ticker          AS ticker_symbol,
        company_name,
        sector,
        exchange,
        'USD'           AS currency
    FROM {{ source('silver', 'stock_prices') }}
    WHERE ticker IS NOT NULL
),

final AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY ticker_symbol) AS ticker_key,
        ticker_symbol,
        company_name,
        sector,
        NULL            AS industry,
        exchange,
        currency,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM source
)

SELECT * FROM final
