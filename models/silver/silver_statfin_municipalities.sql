WITH raw_data AS (
    SELECT
        split(`sourceCode_sourceName_targetCode_targetName_distributionSourceToTarget_distributionTargetToSource`, ';') as cols,
        ingestion_timestamp
    FROM {{ source('statfin_raw', 'bronze_statfin_municipalities') }}
)

SELECT
    TRIM(REGEXP_REPLACE(cols[0], '[\\"\']', '')) AS municipality_code,
    
    CASE 
        WHEN TRIM(REGEXP_REPLACE(cols[0], '[\\"\']', '')) = '989' THEN 'Ähtäri'
        WHEN TRIM(REGEXP_REPLACE(cols[0], '[\\"\']', '')) = '992' THEN 'Äänekoski'
        ELSE TRIM(REGEXP_REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(cols[1], 'Ã„', 'Ä'), 'Ã¤', 'ä'), 'Ã¶', 'ö'), 'Ã¥', 'å'), 
            '[\\"\']', ''))
    END AS municipality,
    
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
WHERE cols[1] NOT LIKE '%sourceName%'
  AND cols[1] IS NOT NULL