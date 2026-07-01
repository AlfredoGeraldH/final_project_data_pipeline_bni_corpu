SELECT
    f.transaction_id,
    f.transaction_code,
    f.fraud_type,
    f.fraud_score,
    f.fraud_level,
    f.flagged_at
FROM dim_frauds f
WHERE f.is_fraud = TRUE
ORDER BY f.fraud_score DESC;