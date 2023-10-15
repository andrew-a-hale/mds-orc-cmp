{{ config(materalized='table') }}
SELECT * FROM '{{ var('csv_file') }}'