DROP TABLE IF EXISTS diff_gord2;
CREATE TABLE diff_gord2 (id INT, v INT);
INSERT INTO diff_gord2 VALUES (1, 10), (2, 21), (3, 32);
SELECT v % 2 AS parity, sum(v) FROM diff_gord2 GROUP BY 1 ORDER BY parity;
SELECT v % 2 AS parity, sum(v) FROM diff_gord2 GROUP BY 2;
SELECT v % 2 AS parity, sum(v) FROM diff_gord2 GROUP BY sum(v);
