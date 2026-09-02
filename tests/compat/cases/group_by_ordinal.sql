DROP TABLE IF EXISTS diff_gord;
CREATE TABLE diff_gord (id INT, v INT);
INSERT INTO diff_gord VALUES (1, 10), (2, 21), (3, 32);
SELECT v % 2 AS parity, sum(v) FROM diff_gord GROUP BY parity ORDER BY parity;
SELECT id / 10 AS bucket, count(*) FROM diff_gord GROUP BY bucket ORDER BY bucket;
SELECT v % 2 AS parity, sum(v) FROM diff_gord GROUP BY 1 ORDER BY parity;
