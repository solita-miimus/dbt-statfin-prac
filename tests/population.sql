-- Test: no negative population totals.
-- A negative population_total indicates a data loading or casting error.
-- This test returns rows that should not exist; dbt fails if any rows are returned.
SELECT
    municipality_code,
    year,
    population_total
FROM {{ ref('fact_population') }}
WHERE population_total < 0