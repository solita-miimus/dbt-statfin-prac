-- dim_year.sql
-- Year dimension. One row per year present in the data (2020-2024).
-- Derived from the population dataset which covers the full year range
-- shared by all fact tables in this project.
SELECT DISTINCT
    year
FROM {{ ref('silver_statfin_population') }}