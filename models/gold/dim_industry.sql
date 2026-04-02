-- dim_industry.sql
-- Industry dimension. One row per distinct industry classification label.
-- Derived from the unique industry values in the bankruptcies data,
-- which is the only fact table with an industry breakdown.
SELECT DISTINCT
    industry
FROM {{ ref('silver_statfin_bankruptcies') }}