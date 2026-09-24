DROP TABLE IF EXISTS diff_peob;
CREATE TABLE diff_peob (g integer, x integer);
INSERT INTO diff_peob VALUES (2, 2), (1, 2), (1, 2), (2, 3), (2, NULL);
SELECT x FROM diff_peob WHERE x = 2 OR x IS NULL ORDER BY g + 1, x NULLS LAST;
SELECT x FROM diff_peob WHERE x = 2 OR x IS NULL ORDER BY g + 1 DESC, x NULLS FIRST;
SELECT x FROM diff_peob WHERE x = 2 OR x IS NULL ORDER BY x NULLS LAST, g + 1 DESC;
SELECT x FROM diff_peob WHERE x = 2 OR x = 3 OR x IS NULL ORDER BY g + 1 DESC, x NULLS LAST;
SELECT x FROM diff_peob WHERE x = 2 OR x = 3 OR x IS NULL ORDER BY x NULLS LAST, g + 1 DESC;
