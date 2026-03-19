SELECT
    CAST(Year AS INT) AS year,
    TRIM(Municipality) AS municipality,
    CAST(Establishments_of_enterprises_number AS INT) AS establishment_count,
    CAST(`Personnel_in_establishments_of_enterprises_staff-years` AS INT) AS personnel_staff_years,
    CAST(Turnover_of_establishments_of_enterprises_EUR_1_000 AS INT) AS turnover_1000e,
    CAST(Turnover_of_establishments_of_enterprises_per_person_EUR_1_000 AS INT) AS turnover_per_person_1000e,
    date_format(ingestion_timestamp, 'yyyyMMddHHmm') AS loaded_at
FROM {{ source('statfin_raw', 'bronze_statfin_establishments') }}