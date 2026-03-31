-- models/gold/dim_year.sql
SELECT DISTINCT
    year
FROM {{ ref('silver_statfin_population') }}