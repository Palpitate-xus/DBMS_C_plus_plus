DROP TABLE IF EXISTS diff_sweep129;
CREATE TABLE diff_sweep129 (g VARCHAR(4), v INT);
INSERT INTO diff_sweep129 VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 15);
SELECT g, v, row_number() OVER (PARTITION BY g ORDER BY v) FROM diff_sweep129 ORDER BY g, v;
SELECT g, v, lag(v) OVER (ORDER BY v) FROM diff_sweep129 ORDER BY v;
SELECT g, v, sum(v) OVER (PARTITION BY g) FROM diff_sweep129 ORDER BY g, v;
