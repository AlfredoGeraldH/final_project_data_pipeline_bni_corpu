TRUNCATE TABLE dim_dates;

INSERT INTO dim_dates (
    date_id,
    full_date,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend,
    is_holiday
)
SELECT DISTINCT ON (full_date)
    CAST(TO_CHAR(full_date, 'YYYYMMDD') AS INTEGER) AS date_id,
    full_date,
    EXTRACT(YEAR FROM full_date)::INT AS year,
    EXTRACT(QUARTER FROM full_date)::INT AS quarter,
    EXTRACT(MONTH FROM full_date)::INT AS month,

    CASE EXTRACT(MONTH FROM full_date)
        WHEN 1 THEN 'Januari'
        WHEN 2 THEN 'Februari'
        WHEN 3 THEN 'Maret'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'Mei'
        WHEN 6 THEN 'Juni'
        WHEN 7 THEN 'Juli'
        WHEN 8 THEN 'Agustus'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'Oktober'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'Desember'
    END AS month_name,

    EXTRACT(WEEK FROM full_date)::INT AS week_of_year,

    EXTRACT(DAY FROM full_date)::INT AS day_of_month,

    -- Monday = 1 ... Sunday = 7
    EXTRACT(ISODOW FROM full_date)::INT AS day_of_week,

    CASE EXTRACT(ISODOW FROM full_date)
        WHEN 1 THEN 'Senin'
        WHEN 2 THEN 'Selasa'
        WHEN 3 THEN 'Rabu'
        WHEN 4 THEN 'Kamis'
        WHEN 5 THEN 'Jumat'
        WHEN 6 THEN 'Sabtu'
        WHEN 7 THEN 'Minggu'
    END AS day_name,

    CASE 
        WHEN EXTRACT(ISODOW FROM full_date) IN (6,7)
        THEN TRUE
        ELSE FALSE
    END AS is_weekend,

    FALSE AS is_holiday

FROM stg_dates
WHERE full_date IS NOT NULL
ORDER BY full_date;