DROP TABLE IF EXISTS diff_han;
CREATE TABLE diff_han (x integer);
SELECT 1 FROM diff_han HAVING sum(x) IS NULL;
SELECT 1 FROM diff_han HAVING sum(x) IS NOT NULL;
INSERT INTO diff_han VALUES (2), (NULL);
SELECT 1 FROM diff_han HAVING sum(x) IS NULL;
SELECT 1 FROM diff_han HAVING sum(x) IS NOT NULL;
SELECT 1 FROM diff_han WHERE x > 2 HAVING sum(x) IS NULL;
SELECT 1 FROM diff_han WHERE x > 2 HAVING sum(x) IS NOT NULL;
