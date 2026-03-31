SELECT
    municipality_code,
    year,
    population_total
FROM {{ ref('fact_population') }}
WHERE population_total < 0