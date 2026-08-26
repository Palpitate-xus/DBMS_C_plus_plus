-- correlated scalar subquery in a GROUP BY select list
DROP TABLE IF EXISTS diff_cs1
DROP TABLE IF EXISTS diff_cs2
CREATE TABLE diff_cs1 (id numeric, g text)
CREATE TABLE diff_cs2 (g text, mult numeric)
INSERT INTO diff_cs1 VALUES (1, 'a'), (2, 'a'), (3, 'b')
INSERT INTO diff_cs2 VALUES ('a', 10), ('b', 100)
SELECT g, count(*), (SELECT mult FROM diff_cs2 WHERE diff_cs2.g = diff_cs1.g) FROM diff_cs1 GROUP BY g ORDER BY g
