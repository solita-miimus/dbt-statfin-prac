-- silver_statfin_bankruptcies.sql
-- Cleans and standardises the raw StatFin bankruptcies table.
-- Resolves municipality_code by joining to silver_statfin_municipalities so
-- downstream gold models can join on a stable key instead of a display name.

WITH municipalities AS (
    -- Single source of truth for municipality_code <-> municipality name mapping.
    SELECT municipality_code, municipality
    FROM {{ ref('silver_statfin_municipalities') }}
)

SELECT
    m.municipality_code,
    CAST(b.Year AS INT) AS year,
    TRIM(b.Industries_luok) AS industry,
    TRIM(b.Municipality) AS municipality,
    CAST(b.Bankruptcies_instigated_number_of_enterprises AS INT) AS number_of_enterprises,
    CAST(b.Bankruptcies_instigated_number_of_employees AS INT) AS number_of_employees,
    date_format(b.ingestion_timestamp, 'yyyyMMddHHmm') AS loaded_at
FROM {{ source('statfin_raw', 'bronze_statfin_bankruptcies') }} b
-- Translate Swedish-language municipality names to their Finnish equivalents
-- so the join matches the municipalities dimension which uses Finnish names.
LEFT JOIN municipalities m ON m.municipality = CASE
    WHEN TRIM(b.Municipality) = 'Ingå'         THEN 'Inkoo'
    WHEN TRIM(b.Municipality) = 'Kimitoön'     THEN 'Kemiönsaari'
    WHEN TRIM(b.Municipality) = 'Kristinestad' THEN 'Kristiinankaupunki'
    WHEN TRIM(b.Municipality) = 'Kronoby'      THEN 'Kruunupyy'
    WHEN TRIM(b.Municipality) = 'Larsmo'       THEN 'Luoto'
    WHEN TRIM(b.Municipality) = 'Malax'        THEN 'Maalahti'
    WHEN TRIM(b.Municipality) = 'Nykarleby'    THEN 'Uusikaarlepyy'
    WHEN TRIM(b.Municipality) = 'Närpes'       THEN 'Närpiö'
    WHEN TRIM(b.Municipality) = 'Pedersöre'    THEN 'Pedersören kunta'
    WHEN TRIM(b.Municipality) = 'Raseborg'     THEN 'Raasepori'
    WHEN TRIM(b.Municipality) = 'Vörå'         THEN 'Vöyri'
    WHEN TRIM(b.Municipality) = 'Mariehamn'    THEN 'Maarianhamina - Mariehamn'
    WHEN TRIM(b.Municipality) = 'Pargas'       THEN 'Parainen'
    WHEN TRIM(b.Municipality) = 'Jakobstad'    THEN 'Pietarsaari'
    WHEN TRIM(b.Municipality) = 'Korsholm'     THEN 'Mustasaari'
    ELSE TRIM(b.Municipality)
END
-- Exclude any municipalities that cannot be resolved to a current code.
WHERE m.municipality_code IS NOT NULL