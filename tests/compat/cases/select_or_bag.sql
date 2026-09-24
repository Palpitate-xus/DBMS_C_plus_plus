DROP TABLE IF EXISTS diff_sob;
CREATE TABLE diff_sob (g integer, x integer);
INSERT INTO diff_sob VALUES (1, 2), (1, 2), (2, 2), (2, 3), (2, NULL);
SELECT x FROM diff_sob WHERE x = 2 OR x = 2 ORDER BY x;
SELECT g, x FROM diff_sob WHERE x = 2 OR g = 1 ORDER BY g, x;
SELECT x FROM diff_sob WHERE x = 2 OR x IS NULL ORDER BY x NULLS LAST;
SELECT x FROM diff_sob WHERE x = 2 OR x IS NOT NULL ORDER BY x;
SELECT x FROM diff_sob WHERE x IS NULL OR x IS NULL ORDER BY x NULLS LAST;
SELECT x FROM diff_sob WHERE (x = 2 OR x IS NULL) AND g = 2 ORDER BY x NULLS LAST;
