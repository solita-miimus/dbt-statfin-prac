WITH raw_data AS (
    SELECT
        split(`sourceCode_sourceName_targetCode_targetName_distributionSourceToTarget_distributionTargetToSource`, ';') as cols,
        ingestion_timestamp
    FROM {{ source('statfin_raw', 'bronze_statfin_municipalities') }}
)

SELECT
    TRIM(REGEXP_REPLACE(
        REPLACE(REPLACE(REPLACE(cols[1], 
            'Ã¤', 'ä'), 
            'Ã¶', 'ö'), 
            'Ã¥', 'å'), 
        '[\\"\']', '')) AS municipality,
    
    TRIM(REGEXP_REPLACE(
        REPLACE(REPLACE(REPLACE(cols[3], 
            'Ã¤', 'ä'), 
            'Ã¶', 'ö'), 
            'Ã¥', 'å'), 
        '[\\"\']', '')) AS region,
        
    date_format(ingestion_timestamp, 'yyyyMMddHHmm') AS loaded_at
FROM raw_data