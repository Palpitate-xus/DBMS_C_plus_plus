-- builtin function NULL semantics and session functions
SELECT concat('a', NULL, 'b')
SELECT concat('x', 'y', NULL)
SELECT current_schema()
SELECT pg_typeof(1)
SELECT pg_typeof(1.5)
SELECT pg_typeof('x')
SELECT generate_series(1, 4)
SELECT generate_series(1, 9, 3)
SELECT generate_series(5, 1, -2)
SELECT generate_series(2, 2)
SELECT i FROM generate_series(1, 3) AS t(i)
SELECT generate_series FROM generate_series(1, 4)
SELECT s FROM generate_series(2, 6, 2) AS t(s)
SELECT sum(i) FROM generate_series(1, 4) AS t(i)
SELECT i FROM generate_series(1, 5) AS t(i) WHERE i > 2 ORDER BY i DESC
SELECT count(*) FROM generate_series(1, 100, 10) AS t(i)
DROP TABLE IF EXISTS n14
CREATE TABLE n14 (a TEXT, b TEXT)
INSERT INTO n14 VALUES ('x', 'y'), ('x', NULL), (NULL, 'y')
SELECT concat(a, b) FROM n14
SELECT concat_ws(':', a, b) FROM n14
DROP TABLE n14
