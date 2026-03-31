SELECT
    m.municipality_code,
    e.year,
    e.establishment_count,
    e.personnel_staff_years,
    e.turnover_1000e,
    e.turnover_per_person_1000e
FROM {{ ref('silver_statfin_establishments') }} e
LEFT JOIN {{ ref('silver_statfin_municipalities') }} m ON e.municipality = m.municipality