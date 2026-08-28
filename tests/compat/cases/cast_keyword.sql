-- keyword CAST forms
SELECT cast(1.5 as numeric(4,2))
SELECT cast(2 as text)
SELECT cast('abc' as varchar(10))
SELECT 1::text
SELECT CAST(2 AS int)
SELECT cast('x' as int)
-- header-naming probes (rows only compared by the runner)
DROP TABLE IF EXISTS ck1
CREATE TABLE ck1 (id INT, v INT)
INSERT INTO ck1 VALUES (1, 5), (2, 9)
SELECT v::text FROM ck1 LIMIT 1
SELECT (v + 1)::text FROM ck1 LIMIT 1
SELECT CAST(v AS int) FROM ck1 LIMIT 1
SELECT v IS NULL FROM ck1 LIMIT 1
SELECT v IS NOT NULL FROM ck1 LIMIT 1
SELECT v IS NULL AS isn FROM ck1 LIMIT 1
DROP TABLE ck1