DROP TABLE IF EXISTS diff_hao;
CREATE TABLE diff_hao (x integer);
INSERT INTO diff_hao VALUES (1), (2), (2), (NULL);
SELECT 1 FROM diff_hao WHERE x = 1 OR x = 2 HAVING count(*) = 3;
SELECT 1 FROM diff_hao WHERE x = 1 OR x = 2 HAVING sum(x) = 5;
SELECT 1 FROM diff_hao WHERE x >= 1 OR x = 2 HAVING count(*) = 3;
SELECT 1 FROM diff_hao WHERE x = 3 OR x = 4 HAVING count(*) = 0;
SELECT 1 FROM diff_hao WHERE x = 3 OR x = 4 HAVING sum(x) IS NULL;
