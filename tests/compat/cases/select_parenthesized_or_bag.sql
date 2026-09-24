DROP TABLE IF EXISTS diff_spob;
CREATE TABLE diff_spob (g integer, x integer);
INSERT INTO diff_spob VALUES (1, 2), (1, 2), (2, 2), (2, NULL);
SELECT x FROM diff_spob WHERE (x = 2 OR x IS NULL) AND g = 1 ORDER BY x;
SELECT x FROM diff_spob WHERE g = 1 AND (x = 2 OR x IS NULL) ORDER BY x;
SELECT x FROM diff_spob WHERE x = 2 OR (x IS NULL AND g = 1) ORDER BY x NULLS LAST;
SELECT g, x FROM diff_spob WHERE (g = 1 OR g = 2) AND x = 2 ORDER BY g, x;
