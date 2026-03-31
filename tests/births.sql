SELECT *
FROM {{ ref('fact_population') }}
WHERE births > population_total