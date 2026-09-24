DROP TABLE IF EXISTS diff_shob;
CREATE TABLE diff_shob (g integer, x integer);
INSERT INTO diff_shob VALUES (2, 2), (1, 2), (1, 2), (2, NULL);
SELECT x + 1 FROM diff_shob WHERE x = 2 OR x IS NULL ORDER BY g, x + 1 NULLS LAST;
SELECT x + 1 FROM diff_shob WHERE x = 2 OR x IS NULL ORDER BY g DESC, x + 1 NULLS FIRST;
SELECT x + 1 FROM diff_shob WHERE x = 2 OR x IS NULL ORDER BY x + 1 DESC NULLS FIRST, g DESC;
