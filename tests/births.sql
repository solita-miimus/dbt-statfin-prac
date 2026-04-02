-- Test: births cannot exceed total population.
-- If births > population_total for any municipality-year, the data is implausible
-- and likely reflects a source error or a join/aggregation issue.
-- This test returns rows that should not exist; dbt fails if any rows are returned.
SELECT *
FROM {{ ref('fact_population') }}
WHERE births > population_total