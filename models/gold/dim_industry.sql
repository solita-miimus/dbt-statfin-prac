-- models/gold/dim_industry.sql
SELECT DISTINCT
    industry
FROM {{ ref('silver_statfin_bankruptcies') }}