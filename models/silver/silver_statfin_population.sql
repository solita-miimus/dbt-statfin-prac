-- silver_statfin_population.sql
-- Cleans and standardises the raw StatFin population table.
-- Key responsibilities:
--   1. Resolve municipality_code by joining to silver_statfin_municipalities on
--      the normalised municipality name. This makes municipality_code available
--      to all downstream gold models without repeating the join there.
--   2. Repair UTF-8 mojibake in the Area field and map Swedish-language bilingual
--      municipality names to their canonical Finnish equivalents so the name join
--      succeeds reliably.
--   3. Cast all numeric columns from string to INT.

WITH municipalities AS (
    -- Single source of truth for municipality_code <-> municipality name mapping.
    SELECT municipality_code, municipality
    FROM {{ ref('silver_statfin_municipalities') }}
)

SELECT
    m.municipality_code,
    CAST(p.Year AS INT) AS year,

    -- Normalise municipality name: repair encoding and translate Swedish-only names
    -- to Finnish so they match the municipalities dimension.
    CASE 
        -- Tehdään siivous ensin ja verrataan puhtaaseen nimeen
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Ingå' THEN 'Inkoo'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Kimitoön' THEN 'Kemiönsaari'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Kristinestad' THEN 'Kristiinankaupunki'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Kronoby' THEN 'Kruunupyy'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Larsmo' THEN 'Luoto'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Malax' THEN 'Maalahti'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Nykarleby' THEN 'Uusikaarlepyy'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Närpes' THEN 'Närpiö'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Pedersöre' THEN 'Pedersören kunta'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Raseborg' THEN 'Raasepori'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Vörå' THEN 'Vöyri'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Mariehamn' THEN 'Maarianhamina - Mariehamn'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Pargas' THEN 'Parainen'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Jakobstad' THEN 'Pietarsaari'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Korsholm' THEN 'Mustasaari'
        
        ELSE TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å'))
    END AS municipality,

    -- Loput väestösarakkeet (births, deaths jne.) tulevat tähän perään kuten aiemmin
    CAST(Live_births AS INT) AS births,
    CAST(Deaths AS INT) AS deaths,
    CAST(Natural_increase AS INT) AS natural_increase,
    CAST(`Intermunicipal_in-migration` AS INT) AS migration_in_internal,
    CAST(`Intermunicipal_out-migration` AS INT) AS migration_out_internal,
    CAST(Intermunicipal_net_migration AS INT) AS net_migration_internal,
    CAST(Intramunicipal_migration AS INT) AS migration_within_municipality,
    CAST(Immigration_to_Finland AS INT) AS migration_in_international,
    CAST(Emigration_from_Finland AS INT) AS migration_out_international,
    CAST(Net_migration AS INT) AS net_migration_international,
    CAST(Total_net_migration AS INT) AS net_migration_total,
    CAST(Population_increase AS INT) AS population_increase,
    CAST(Total_change AS INT) AS total_change_count,
    CAST(Population AS INT) AS population_total,
    CAST(Marriages AS INT) AS marriages,
    CAST(Divorces AS INT) AS divorces,
    date_format(p.ingestion_timestamp, 'yyyyMMddHHmm') AS loaded_at

FROM {{ source('statfin_raw', 'bronze_statfin_population') }} p
LEFT JOIN municipalities m ON m.municipality = CASE
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Ingå' THEN 'Inkoo'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Kimitoön' THEN 'Kemiönsaari'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Kristinestad' THEN 'Kristiinankaupunki'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Kronoby' THEN 'Kruunupyy'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Larsmo' THEN 'Luoto'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Malax' THEN 'Maalahti'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Nykarleby' THEN 'Uusikaarlepyy'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Närpes' THEN 'Närpiö'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Pedersöre' THEN 'Pedersören kunta'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Raseborg' THEN 'Raasepori'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Vörå' THEN 'Vöyri'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Mariehamn' THEN 'Maarianhamina - Mariehamn'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Pargas' THEN 'Parainen'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Jakobstad' THEN 'Pietarsaari'
        WHEN TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å')) = 'Korsholm' THEN 'Mustasaari'
        ELSE TRIM(REPLACE(REPLACE(REPLACE(REPLACE(Area, 'Ã', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å'))
    END