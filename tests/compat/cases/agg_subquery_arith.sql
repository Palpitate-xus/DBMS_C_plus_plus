-- aggregate combined with correlated scalar subquery in GROUP BY
DROP TABLE IF EXISTS diff_cs1
DROP TABLE IF EXISTS diff_cs2
CREATE TABLE diff_cs1 (id numeric, g text)
CREATE TABLE diff_cs2 (g text, mult numeric)
INSERT INTO diff_cs1 VALUES (1, 'a'), (2, 'a'), (3, 'b')
INSERT INTO diff_cs2 VALUES ('a', 10), ('b', 100)
SELECT g, sum(id) + (SELECT mult FROM diff_cs2 WHERE diff_cs2.g = diff_cs1.g) FROM diff_cs1 GROUP BY g ORDER BY g
SELECT g, count(*) * (SELECT mult FROM diff_cs2 WHERE diff_cs2.g = diff_cs1.g) FROM diff_cs1 GROUP BY g ORDER BY g
-- aggregate-vs-aggregate arithmetic (no subquery operand)
DROP TABLE IF EXISTS diff_aa1
CREATE TABLE diff_aa1 (id INT, v INT)
INSERT INTO diff_aa1 VALUES (1, 10), (2, 20), (3, 30)
SELECT count(*) - count(v) FROM diff_aa1
SELECT count(*) + 1 FROM diff_aa1
SELECT sum(v) / count(*) FROM diff_aa1
SELECT max(v) - min(v) FROM diff_aa1
SELECT avg(v) * 2 FROM diff_aa1
SELECT count(*) AS n, count(v) AS nv FROM diff_aa1
DROP TABLE diff_aa1
