-- explicit and implicit casts
SELECT CAST(1 AS text)
SELECT CAST('42' AS integer)
SELECT CAST(3.7 AS int)
SELECT CAST(-3.7 AS int)
SELECT 1::text
SELECT '7'::int
SELECT '2020-01-02'::date
SELECT CAST(true AS int)
SELECT CAST(1 AS boolean)
