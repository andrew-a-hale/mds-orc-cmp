{{ config(materalized='table') }}
SELECT country, AVG(p25), AVG(p50), AVG(p75), unixepoch('now')
FROM salaries
GROUP BY country