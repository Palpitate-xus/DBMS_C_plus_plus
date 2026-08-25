-- subquery semantics
DROP TABLE IF EXISTS diff_sq
CREATE TABLE diff_sq (g text, v numeric)
INSERT INTO diff_sq VALUES ('a', 1), ('a', 2), ('b', 3)
SELECT * FROM diff_sq WHERE v > (SELECT min(v) FROM diff_sq) ORDER BY v
SELECT g, sum(v) FROM diff_sq GROUP BY g HAVING sum(v) > 2 ORDER BY g
SELECT v FROM diff_sq WHERE v IN (1, 3) ORDER BY v
SELECT v FROM diff_sq WHERE v NOT IN (1) ORDER BY v
SELECT (SELECT count(*) FROM diff_sq) AS c
