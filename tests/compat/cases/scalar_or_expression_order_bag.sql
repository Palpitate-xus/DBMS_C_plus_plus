DROP TABLE IF EXISTS diff_soeb;
CREATE TABLE diff_soeb (g integer, x integer);
INSERT INTO diff_soeb VALUES (1, 2), (1, 2), (2, 2), (2, 3), (2, NULL);
SELECT x + 1 FROM diff_soeb WHERE x = 2 OR x IS NULL ORDER BY x + 1 NULLS LAST;
SELECT x + 1 FROM diff_soeb WHERE x = 2 OR x IS NULL ORDER BY x + 1 DESC NULLS FIRST;
SELECT x + 1 FROM diff_soeb ORDER BY x + 1 DESC NULLS FIRST;
