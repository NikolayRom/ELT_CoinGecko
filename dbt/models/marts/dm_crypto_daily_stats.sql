{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['report_date', 'coin_id']
) }}

WITH daily_grouped AS (
    SELECT
        toDate(last_updated_at) AS report_date,
        coin_id,
        min(price_usd) AS min_price_usd,
        max(price_usd) AS max_price_usd,
        avg(price_usd) AS avg_price_usd,
        argMax(market_cap_usd, last_updated_at) AS latest_market_cap_usd,
        count(*) AS ticks_count
    FROM {{ ref('stg_crypto_prices') }}
    GROUP BY
        report_date,
        coin_id
)

SELECT
    report_date,
    coin_id,
    min_price_usd,
    max_price_usd,
    avg_price_usd,
    latest_market_cap_usd,
    ticks_count,
    round((toFloat64(max_price_usd - min_price_usd) / toFloat64(min_price_usd)) * 100, 2) AS volatility_pct
FROM daily_grouped