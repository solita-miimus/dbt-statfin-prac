-- fact_bankruptcies.sql
-- Bankruptcy fact table. Grain: one row per municipality per industry per year.
-- Includes the number of bankrupt enterprises and affected employees, plus
-- year-over-year change metrics calculated as window functions.
-- municipality_code is resolved in the silver layer; no join needed here.
WITH base AS (
    SELECT 
        municipality_code,
        industry,
        year,
        number_of_enterprises AS bankruptcies_count,
        number_of_employees AS personnel_at_risk
    FROM {{ ref('silver_statfin_bankruptcies') }}
)

SELECT
    *,
    -- Absolute change in bankruptcies compared to the previous year,
    -- partitioned by municipality and industry to keep trends meaningful.
    bankruptcies_count - LAG(bankruptcies_count) OVER (
        PARTITION BY municipality_code, industry 
        ORDER BY year
    ) AS bankruptcies_change_abs,

    -- Absolute change in employees at risk compared to the previous year.
    personnel_at_risk - LAG(personnel_at_risk) OVER (
        PARTITION BY municipality_code, industry 
        ORDER BY year
    ) AS personnel_at_risk_change_abs
FROM base