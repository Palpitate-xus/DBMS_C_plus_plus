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
-- cast column headers name the target type (PG: float8, text)
DROP TABLE IF EXISTS diff_cst
CREATE TABLE diff_cst (v numeric)
INSERT INTO diff_cst VALUES (1.5), (2.5)
SELECT CAST(v AS double precision) FROM diff_cst
SELECT CAST(v AS text) FROM diff_cst
SELECT CAST(v AS integer) FROM diff_cst
-- unary sign projection
SELECT -v FROM diff_cst
SELECT +v FROM diff_cst
SELECT -v - 1 FROM diff_cst
