-- fact_population.sql
-- Population fact table. Grain: one row per municipality per year.
-- Includes demographic measures (births, deaths, migration, marriages, divorces)
-- and a derived year-over-year population change calculated as a window function.
-- municipality_code is resolved in the silver layer; no join needed here.
SELECT
    municipality_code,
    year,
    population_total,
    births,
    deaths,
    net_migration_total,
    natural_increase,
    marriages,
    divorces,
    -- Absolute population change compared to the previous year for the same municipality.
    population_total - LAG(population_total) OVER (PARTITION BY municipality_code ORDER BY year) AS population_change_abs
FROM {{ ref('silver_statfin_population') }}