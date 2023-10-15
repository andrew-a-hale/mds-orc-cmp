SELECT row_count
FROM (
    SELECT COUNT(*) AS row_count
    FROM salaries
    WHERE loaded_at >= {{ loaded_at }} AND country = {{ country }}
) X
WHERE row_count > 0