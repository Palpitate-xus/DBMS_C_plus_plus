DROP TABLE IF EXISTS diff_gexpr;
CREATE TABLE diff_gexpr (id INT, v INT);
INSERT INTO diff_gexpr VALUES (1, 10), (2, 21), (3, 32);
SELECT v % 2 AS parity, sum(v) FROM diff_gexpr GROUP BY v % 2 ORDER BY parity;
SELECT id / 10 AS bucket, count(*) FROM diff_gexpr GROUP BY id / 10 ORDER BY bucket;
SELECT v + 1 AS w, sum(v) FROM diff_gexpr GROUP BY v + 1 ORDER BY w;
