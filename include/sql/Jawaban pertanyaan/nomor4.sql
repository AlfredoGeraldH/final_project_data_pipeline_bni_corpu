--Channel
SELECT
    c.channel_name,
    c.channel_category,
    COUNT(f.transaction_id)
        AS transaction_volume,
    SUM(f.amount)
        AS transaction_value
FROM fact_transactions f
JOIN dim_channels c
    ON f.channel_id = c.channel_id
GROUP BY
    c.channel_name,
    c.channel_category
ORDER BY transaction_volume DESC;

--Digital Migration Trend
SELECT
    d.year,
    d.month_name,
    c.is_digital,
    COUNT(f.transaction_id)
        AS transaction_volume,
    SUM(f.amount)
        AS transaction_value
FROM fact_transactions f
JOIN dim_dates d
    ON f.transaction_date_id = d.date_id
JOIN dim_channels c
    ON f.channel_id = c.channel_id
GROUP BY
    d.year,
    d.month,
    d.month_name,
    c.is_digital
ORDER BY
    d.year,
    d.month;