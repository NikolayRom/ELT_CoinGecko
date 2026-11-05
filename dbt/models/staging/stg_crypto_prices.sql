{{ config(materialized='view') }}

SELECT
    coin_id,
    price_usd,
    market_cap_usd,
    last_updated_at,
    extracted_at
FROM {{ source('raw_crypto', 'silver_crypto_prices') }}
FINAL
WHERE price_usd > 0