--Daily Transaction
SELECT
    d.full_date,
    COUNT(f.transaction_id) AS transaction_volume,
    SUM(f.amount) AS total_transaction_value
FROM fact_transactions f
JOIN dim_dates d
    ON f.transaction_date_id = d.date_id
GROUP BY d.full_date
ORDER BY d.full_date;

--Weekly Transaction
SELECT
    d.year,
    d.week_of_year,
    COUNT(f.transaction_id) AS transaction_volume,
    SUM(f.amount) AS total_transaction_value
FROM fact_transactions f
JOIN dim_dates d
    ON f.transaction_date_id = d.date_id
GROUP BY
    d.year,
    d.week_of_year
ORDER BY
    d.year,
    d.week_of_year;

--Monthly Transaction
WITH monthly_transaction AS (
    SELECT
        d.year,
        d.month,
        d.month_name,
        COUNT(f.transaction_id) AS transaction_volume,
        SUM(f.amount) AS total_value
    FROM fact_transactions f
    JOIN dim_dates d
        ON f.transaction_date_id = d.date_id
    GROUP BY
        d.year,
        d.month,
        d.month_name
)

SELECT
    *,
    LAG(total_value)
        OVER(
            ORDER BY year, month
        ) AS previous_month_value,
    (
        total_value -
        LAG(total_value)
        OVER(
            ORDER BY year, month
        )
    )
    /
    NULLIF(
        LAG(total_value)
        OVER(
            ORDER BY year, month
        ),0
    ) * 100 AS growth_percentage
FROM monthly_transaction;