DROP TABLE IF EXISTS diff_haw;
CREATE TABLE diff_haw (x integer);
INSERT INTO diff_haw VALUES (1), (3), (NULL);
SELECT 1 FROM diff_haw WHERE x > 2 HAVING count(*) = 1;
SELECT 1 FROM diff_haw WHERE x > 2 HAVING sum(x) = 3;
SELECT 1 FROM diff_haw WHERE x > 4 HAVING count(*) = 0;
SELECT 1 FROM diff_haw WHERE x > 4 HAVING sum(x) > 0;
SELECT 1 FROM diff_haw WHERE x > 2 HAVING count(x) = 0;
