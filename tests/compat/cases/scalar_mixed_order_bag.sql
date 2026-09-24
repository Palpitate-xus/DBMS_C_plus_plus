DROP TABLE IF EXISTS diff_smob;
CREATE TABLE diff_smob (g integer, x integer);
INSERT INTO diff_smob VALUES (1, 2), (1, 2), (2, 2), (2, 3), (2, NULL);
SELECT g, x + 1 FROM diff_smob WHERE g = 1 OR x = 3 ORDER BY g, x + 1;
SELECT g, x + 1 FROM diff_smob WHERE x = 2 OR x IS NULL ORDER BY g DESC, x + 1 NULLS LAST;
SELECT g, x + 1 FROM diff_smob WHERE x = 2 OR x IS NULL ORDER BY x + 1 DESC NULLS FIRST, g DESC;
