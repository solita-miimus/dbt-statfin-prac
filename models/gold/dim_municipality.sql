-- dim_municipality.sql
-- Municipality dimension. One row per municipality.
-- Exposes municipality_code (PK), the Finnish name, and the wellbeing services
-- county the municipality belongs to. Sourced from silver_statfin_municipalities
-- which handles all encoding cleanup and name normalisation.
SELECT
    municipality_code,
    municipality,
    region AS wellbeing_county,  -- renamed for clarity in the business layer
    loaded_at
FROM {{ ref('silver_statfin_municipalities') }}