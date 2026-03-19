SELECT
    CAST(Year AS INT) AS year,

    -- Väestön luonnollinen muutos
    CAST(Live_births AS INT) AS births,
    CAST(Deaths AS INT) AS deaths,
    CAST(Natural_increase AS INT) AS natural_increase,

    -- Kotimaan muuttoliike (Kuntien välinen)
    CAST(`Intermunicipal_in-migration` AS INT) AS migration_in_internal,
    CAST(`Intermunicipal_out-migration` AS INT) AS migration_out_internal,
    CAST(Intermunicipal_net_migration AS INT) AS net_migration_internal,
    CAST(Intramunicipal_migration AS INT) AS migration_within_municipality,

    -- Kansainvälinen muuttoliike
    CAST(Immigration_to_Finland AS INT) AS migration_in_international,
    CAST(Emigration_from_Finland AS INT) AS migration_out_international,
    CAST(Net_migration AS INT) AS net_migration_international,

    -- Kokonaismuutokset
    CAST(Total_net_migration AS INT) AS net_migration_total,
    CAST(Population_increase AS INT) AS population_increase,
    CAST(Total_change AS INT) AS total_change_count,
    CAST(Population AS INT) AS population_total,

    -- Siviilisäätyjen muutokset
    CAST(Marriages AS INT) AS marriages,
    CAST(Divorces AS INT) AS divorces,

    date_format(ingestion_timestamp, 'yyyyMMddHHmm') AS loaded_at

FROM {{ source('statfin_raw', 'bronze_statfin_population') }}