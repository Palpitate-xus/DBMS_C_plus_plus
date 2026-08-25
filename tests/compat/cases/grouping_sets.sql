-- grouping sets: ROLLUP / CUBE / GROUPING SETS
DROP TABLE IF EXISTS diff_gs
CREATE TABLE diff_gs (g text, d text, v numeric)
INSERT INTO diff_gs VALUES ('a', 'x', 10), ('a', 'y', 20), ('b', 'x', 5)
SELECT g, sum(v) FROM diff_gs GROUP BY ROLLUP (g) ORDER BY g
SELECT g, sum(v) FROM diff_gs GROUP BY ROLLUP(g) ORDER BY g
