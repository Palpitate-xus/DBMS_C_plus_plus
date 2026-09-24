DROP TABLE IF EXISTS diff_gof;
CREATE TABLE diff_gof (g integer, x integer);
INSERT INTO diff_gof VALUES (1, 1), (1, 2), (1, 2), (2, 2), (2, 3), (2, NULL);
SELECT g, count(*) FROM diff_gof WHERE x = 1 OR x = 2 GROUP BY g ORDER BY g;
SELECT g, sum(x) FROM diff_gof WHERE x >= 1 OR x = 2 GROUP BY g ORDER BY g;
SELECT g, count(*) FROM diff_gof WHERE x = 9 OR x = 10 GROUP BY g ORDER BY g;
