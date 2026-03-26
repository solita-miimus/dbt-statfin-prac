WITH municipalities AS (
    SELECT * FROM {{ ref('silver_statfin_municipalities') }}
),

population AS (
    SELECT * FROM {{ ref('silver_statfin_population') }}
    -- Otetaan vain tuorein vuosi analyysiin, jotta data on selkeää
    WHERE year = (SELECT MAX(year) FROM {{ ref('silver_statfin_population') }})
)

SELECT
    m.municipality_code,
    m.municipality,
    m.region,
    p.year,
    p.population_total,
    p.net_migration_total,
    p.births,
    p.deaths,
    -- Lasketaan elinvoimaisuusindeksi (Births / Deaths)
    ROUND(p.births / NULLIF(p.deaths, 0), 2) AS vitality_index,
    p.loaded_at
FROM municipalities m
LEFT JOIN population p 
    ON m.municipality = p.municipality