-- Transform: stg_channels → dim_channels
-- Cast tipe data, tambah derived columns, deduplikasi

TRUNCATE TABLE dim_channels;

INSERT INTO dim_channels (
    channel_id,
    channel_code,
    channel_name,
    channel_category,
    is_digital,
    description
)
SELECT DISTINCT ON (channel_id)
    channel_id,
    CASE
        WHEN channel_name = 'ATM' THEN 'ATM'
        WHEN channel_name = 'Mobile Banking' THEN 'MB'
        WHEN channel_name = 'Internet Banking' THEN 'IB'
        WHEN channel_name = 'Teller' THEN 'TELLER'
        WHEN channel_name = 'EDC / Mesin Kasir' THEN 'EDC'
        WHEN channel_name = 'Call Center / IVR,DIGITAL' THEN 'CS'
        ELSE 'NULL'
    END AS channel_code,
    channel_name,
    CASE
        WHEN channel_code IN ('ATM', 'TELLER', 'EDC') THEN 'PHYSICAL'
        WHEN channel_code IN ('MB', 'IB', 'CS') THEN 'DIGITAL'
        ELSE NULL
    END AS channel_category,
    CASE
        WHEN channel_category = 'DIGITAL' THEN True ELSE False
    END AS is_digital,
    description
FROM stg_channels
WHERE channel_id IS NOT NULL
ORDER BY channel_id;