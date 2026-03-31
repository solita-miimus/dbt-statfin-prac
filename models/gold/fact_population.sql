-- models/gold/fct_population.sql
SELECT
    m.municipality_code,
    p.year,
    p.population_total,
    p.births,
    p.deaths,
    p.net_migration_total,
    p.natural_increase,
    p.marriages,
    p.divorces,
    -- Laskettu muutos
    p.population_total - LAG(p.population_total) OVER (PARTITION BY p.municipality ORDER BY p.year) AS population_change_abs
FROM {{ ref('silver_statfin_population') }} p
LEFT JOIN {{ ref('silver_statfin_municipalities') }} m ON p.municipality = m.municipality