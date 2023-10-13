SELECT row_count
FROM (
    SELECT COUNT(*) AS row_count
    FROM agg
    WHERE loaded_at >= {{ loaded_at }}
)
WHERE row_count > 0