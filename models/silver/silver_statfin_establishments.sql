-- silver_statfin_establishments.sql
-- Cleans and standardises the raw StatFin business establishments table.
-- Resolves municipality_code by joining to silver_statfin_municipalities so
-- downstream gold models can join on a stable key instead of a display name.

WITH municipalities AS (
    -- Single source of truth for municipality_code <-> municipality name mapping.
    SELECT municipality_code, municipality
    FROM {{ ref('silver_statfin_municipalities') }}
)

SELECT
    m.municipality_code,
    CAST(e.Year AS INT) AS year,
    TRIM(e.Municipality) AS municipality,
    CAST(e.Establishments_of_enterprises_number AS INT) AS establishment_count,
    CAST(e.`Personnel_in_establishments_of_enterprises_staff-years` AS INT) AS personnel_staff_years,
    CAST(e.Turnover_of_establishments_of_enterprises_EUR_1_000 AS INT) AS turnover_1000e,
    CAST(e.Turnover_of_establishments_of_enterprises_per_person_EUR_1_000 AS INT) AS turnover_per_person_1000e,
    date_format(e.ingestion_timestamp, 'yyyyMMddHHmm') AS loaded_at
FROM {{ source('statfin_raw', 'bronze_statfin_establishments') }} e
-- Translate Swedish-language municipality names to their Finnish equivalents
-- so the join matches the municipalities dimension which uses Finnish names.
LEFT JOIN municipalities m ON m.municipality = CASE
    WHEN TRIM(e.Municipality) = 'Ingå'         THEN 'Inkoo'
    WHEN TRIM(e.Municipality) = 'Kimitoön'     THEN 'Kemiönsaari'
    WHEN TRIM(e.Municipality) = 'Kristinestad' THEN 'Kristiinankaupunki'
    WHEN TRIM(e.Municipality) = 'Kronoby'      THEN 'Kruunupyy'
    WHEN TRIM(e.Municipality) = 'Larsmo'       THEN 'Luoto'
    WHEN TRIM(e.Municipality) = 'Malax'        THEN 'Maalahti'
    WHEN TRIM(e.Municipality) = 'Nykarleby'    THEN 'Uusikaarlepyy'
    WHEN TRIM(e.Municipality) = 'Närpes'       THEN 'Närpiö'
    WHEN TRIM(e.Municipality) = 'Pedersöre'    THEN 'Pedersören kunta'
    WHEN TRIM(e.Municipality) = 'Raseborg'     THEN 'Raasepori'
    WHEN TRIM(e.Municipality) = 'Vörå'         THEN 'Vöyri'
    WHEN TRIM(e.Municipality) = 'Mariehamn'    THEN 'Maarianhamina - Mariehamn'
    WHEN TRIM(e.Municipality) = 'Pargas'       THEN 'Parainen'
    WHEN TRIM(e.Municipality) = 'Jakobstad'    THEN 'Pietarsaari'
    WHEN TRIM(e.Municipality) = 'Korsholm'     THEN 'Mustasaari'
    ELSE TRIM(e.Municipality)
END
-- Exclude dissolved municipalities that no longer exist in the dimension
-- and would produce null municipality_code in downstream gold models.
WHERE m.municipality_code IS NOT NULL