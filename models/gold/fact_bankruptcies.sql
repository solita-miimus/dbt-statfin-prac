-- models/gold/fact_bankruptcies.sql
WITH base AS (
    SELECT 
        m.municipality_code,
        b.industry,
        b.year,
        b.number_of_enterprises AS bankruptcies_count,
        b.number_of_employees AS personnel_at_risk
    FROM {{ ref('silver_statfin_bankruptcies') }} b
    LEFT JOIN {{ ref('silver_statfin_municipalities') }} m ON b.municipality = m.municipality
)

SELECT
    *,
    -- Konkurssien muutos edelliseen vuoteen (yritykset)
    bankruptcies_count - LAG(bankruptcies_count) OVER (
        PARTITION BY municipality_code, industry 
        ORDER BY year
    ) AS bankruptcies_change_abs,

    -- Konkurssien muutos edelliseen vuoteen (henkilöstö)
    personnel_at_risk - LAG(personnel_at_risk) OVER (
        PARTITION BY municipality_code, industry 
        ORDER BY year
    ) AS personnel_at_risk_change_abs
FROM base