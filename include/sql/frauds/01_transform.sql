-- Transform: stg_frauds → dim_frauds
-- Cast tipe data, tambah fraud classification, deduplikasi

TRUNCATE TABLE dim_frauds;
INSERT INTO dim_frauds
(
    transaction_id,
    transaction_code,
    is_fraud,
    fraud_type,
    fraud_score,
    fraud_level,
    flagged_at
)

SELECT DISTINCT ON (transaction_id)
    transaction_id,
    transaction_code,
    CASE
        WHEN LOWER(is_fraud) = 'true'
        THEN TRUE
        ELSE FALSE
    END AS is_fraud,
    fraud_type,
    fraud_score::NUMERIC(5,4),
    CASE
        WHEN fraud_score::NUMERIC >= 0.90
            THEN 'HIGH'
        WHEN fraud_score::NUMERIC >= 0.75
            THEN 'MEDIUM'
        ELSE 'LOW'
    END AS fraud_level,
    flagged_at::TIMESTAMP
FROM stg_frauds
WHERE transaction_id IS NOT NULL
ORDER BY transaction_id;