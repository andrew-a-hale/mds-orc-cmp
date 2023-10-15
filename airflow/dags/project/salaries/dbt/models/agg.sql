{{ config(materalized='table') }}
SELECT country, AVG(p25), AVG(p50), AVG(p75), epoch(current_timestamp)
FROM salaries
GROUP BY country