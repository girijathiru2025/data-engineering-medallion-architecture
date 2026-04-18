{{
    config(
        materialized='incremental',
        unique_key=['date_key', 'ticker_key'],
        on_schema_change='fail'
    )
}}

/*
  Gold Layer: fact_stock_prices
  --------------------------------
  Central fact table in the star schema.
  Joins Silver stock prices with dimension tables to produce
  a fully resolved, analytics-ready dataset.

  Incremental strategy: merge on (date_key, ticker_key) so daily
  runs only process new or updated records.
*/

WITH silver AS (
    SELECT *
    FROM {{ source('silver', 'stock_prices') }}

    {% if is_incremental() %}
    WHERE processed_at::TIMESTAMP WITH TIME ZONE > COALESCE(
        (SELECT MAX(created_at) FROM {{ this }}),
        '1970-01-01'::TIMESTAMP WITH TIME ZONE
    )
    {% endif %}
),

joined AS (
    SELECT
        dd.date_key,
        dt.ticker_key,
        s.open          AS open_price,
        s.high          AS high_price,
        s.low           AS low_price,
        s.close         AS close_price,
        s.adj_close     AS adj_close_price,
        s.volume,
        s.daily_return,
        s.price_range,
        CURRENT_TIMESTAMP AS created_at
    FROM silver s
    INNER JOIN {{ ref('dim_date') }}   dd ON dd.full_date    = s.date
    INNER JOIN {{ ref('dim_ticker') }} dt ON dt.ticker_symbol = s.ticker
)

SELECT * FROM joined
