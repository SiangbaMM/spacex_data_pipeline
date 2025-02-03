-- Example query showing launch types
SELECT
    l.launch_id,
    CASE
        WHEN d.dragon_key IS NULL THEN 'Non-Dragon Launch'
        WHEN d.type = 'Cargo' THEN 'Dragon Cargo Mission'
        WHEN d.type = 'Crew' THEN 'Dragon Crew Mission'
    END as mission_type
FROM
    FACT_LAUNCHES l
    LEFT JOIN DIM_DRAGONS d ON l.dragon_key = d.dragon_key;

-- Track Dragon reuse
SELECT
    d.name as dragon_name,
    COUNT(DISTINCT l.launch_key) as total_missions
FROM
    DIM_DRAGONS d
    JOIN FACT_LAUNCHES l ON d.dragon_key = l.dragon_key
GROUP BY
    d.name;
