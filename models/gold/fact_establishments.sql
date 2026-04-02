-- fact_establishments.sql
-- Business establishments fact table. Grain: one row per municipality per year.
-- Includes the count of establishments, personnel (staff-years), and turnover.
-- municipality_code is resolved in the silver layer; no join needed here.
SELECT
    municipality_code,
    year,
    establishment_count,
    personnel_staff_years,
    turnover_1000e,
    turnover_per_person_1000e
FROM {{ ref('silver_statfin_establishments') }}