-- ORDER BY over aggregate output (alias, expression, group column)
DROP TABLE IF EXISTS diff_oa
CREATE TABLE diff_oa (g text, v numeric)
INSERT INTO diff_oa VALUES ('a', 10), ('a', 20), ('b', 5)
SELECT g, count(*) AS cn FROM diff_oa GROUP BY g ORDER BY cn
SELECT g, count(*) AS cn FROM diff_oa GROUP BY g ORDER BY cn DESC
SELECT g, sum(v) AS sv FROM diff_oa GROUP BY g ORDER BY sv
SELECT g, count(*) AS cn FROM diff_oa GROUP BY g ORDER BY g DESC
SELECT g, count(*) FROM diff_oa GROUP BY g ORDER BY count(*)
