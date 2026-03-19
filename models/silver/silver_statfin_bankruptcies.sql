SELECT
    CAST(Year AS INT) AS year,
    TRIM(Industries_luok) AS industry,
    TRIM(Municipality) AS municipality,
    CAST(Bankruptcies_instigated_number_of_enterprises AS INT) AS number_of_enterprises,
    CAST(Bankruptcies_instigated_number_of_employees AS INT) AS number_of_employees,
    date_format(ingestion_timestamp, 'yyyyMMddHHmm') AS loaded_at
FROM {{ source('statfin_raw', 'bronze_statfin_bankruptcies') }}