DROP TABLE IF EXISTS diff_gsexpr;
CREATE TABLE diff_gsexpr (id INT, v INT);
INSERT INTO diff_gsexpr VALUES (1, 10), (2, 21), (3, 32);
SELECT id / 10 AS bucket, sum(v) FROM diff_gsexpr GROUP BY GROUPING SETS ((id / 10), ()) ORDER BY bucket;
SELECT v % 2 AS p, id / 10 AS b, sum(v) FROM diff_gsexpr GROUP BY GROUPING SETS ((v % 2), (id / 10), ()) ORDER BY p, b;
SELECT v % 2 AS p, sum(v) FROM diff_gsexpr GROUP BY ROLLUP (v % 2) ORDER BY p;
SELECT id / 10 AS b, count(*) FROM diff_gsexpr GROUP BY CUBE (id / 10) ORDER BY b;
