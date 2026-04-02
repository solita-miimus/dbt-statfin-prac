-- silver_statfin_municipalities.sql
-- Parses the raw municipality-to-wellbeing-county mapping from StatFin.
-- The source is a CSV-style string stored in a single column; it is split by
-- semicolon and cleaned of surrounding quotes and UTF-8 mojibake sequences
-- (e.g. 'Ã„' -> 'Ä') that appear due to encoding issues in the raw load.
-- Two municipality codes (989, 992) are hard-coded overrides because the source
-- name field contains encoding artifacts that cannot be repaired generically.
-- The header row is filtered out via the NOT LIKE '%sourceName%' guard.

WITH raw_data AS (
    SELECT
        -- The source stores the entire row as a semicolon-delimited string;
        -- split() turns it into an array so each field can be accessed by index.
        split(`sourceCode_sourceName_targetCode_targetName_distributionSourceToTarget_distributionTargetToSource`, ';') as cols,
        ingestion_timestamp
    FROM {{ source('statfin_raw', 'bronze_statfin_municipalities') }}
)

SELECT
    -- cols[0]: municipality code (numeric string, e.g. '091' for Helsinki)
    TRIM(REGEXP_REPLACE(cols[0], '[\\"\']', '')) AS municipality_code,
    
    -- cols[1]: municipality name in Finnish.
    -- Two codes need name overrides because the source has unresolvable encoding.
    -- All other names are cleaned with REPLACE chains for common mojibake patterns.
    CASE 
        WHEN TRIM(REGEXP_REPLACE(cols[0], '[\\"\']', '')) = '989' THEN 'Ähtäri'
        WHEN TRIM(REGEXP_REPLACE(cols[0], '[\\"\']', '')) = '992' THEN 'Äänekoski'
        ELSE TRIM(REGEXP_REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(cols[1], 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å'), 
            '[\\"\']', ''))
    END AS municipality,
    
    -- cols[3]: name of the wellbeing services county the municipality belongs to.
    -- Same mojibake cleanup applied; an extra pattern covers 'Ö' (Ã–).
    TRIM(REGEXP_REPLACE(
        REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(cols[3], 
            'Ã„', 'Ä'), 
            'Ã¤', 'ä'), 
            'Ã¶', 'ö'), 
            'Ã¥', 'å'),
            'Ã–', 'Ö'),
        '[\\"\']', '')) AS region,
        
    date_format(ingestion_timestamp, 'yyyyMMddHHmm') AS loaded_at
FROM raw_data
WHERE cols[1] NOT LIKE '%sourceName%'  -- skip the header row
  AND cols[1] IS NOT NULL               -- skip empty rows