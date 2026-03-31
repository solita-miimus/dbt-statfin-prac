-- models/gold/dim_municipality.sql
SELECT
    municipality_code,
    municipality,
    region AS wellbeing_county,
    loaded_at
FROM {{ ref('silver_statfin_municipalities') }}