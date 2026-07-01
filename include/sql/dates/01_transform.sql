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
SELECT DISTINCT ON (full_date::DATE)

    CAST(TO_CHAR(full_date::DATE, 'YYYYMMDD') AS INTEGER) AS date_id,

    full_date::DATE,

    EXTRACT(YEAR FROM full_date::DATE)::INT,

    EXTRACT(QUARTER FROM full_date::DATE)::INT,

    EXTRACT(MONTH FROM full_date::DATE)::INT,

    CASE EXTRACT(MONTH FROM full_date::DATE)
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
    END,

    EXTRACT(WEEK FROM full_date::DATE)::INT,

    EXTRACT(DAY FROM full_date::DATE)::INT,

    EXTRACT(ISODOW FROM full_date::DATE)::INT,

    CASE EXTRACT(ISODOW FROM full_date::DATE)
        WHEN 1 THEN 'Senin'
        WHEN 2 THEN 'Selasa'
        WHEN 3 THEN 'Rabu'
        WHEN 4 THEN 'Kamis'
        WHEN 5 THEN 'Jumat'
        WHEN 6 THEN 'Sabtu'
        WHEN 7 THEN 'Minggu'
    END,

    CASE 
        WHEN EXTRACT(ISODOW FROM full_date::DATE) IN (6,7)
        THEN TRUE
        ELSE FALSE
    END,

    FALSE

FROM stg_dates

WHERE full_date IS NOT NULL

ORDER BY full_date::DATE;