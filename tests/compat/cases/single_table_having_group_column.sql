DROP TABLE IF EXISTS diff_having_group;
CREATE TABLE diff_having_group (v int);
INSERT INTO diff_having_group VALUES (-1), (1), (2), (NULL);
SELECT v, count(*) FROM diff_having_group GROUP BY v HAVING v > 0 ORDER BY v;
SELECT v, count(*) FROM diff_having_group GROUP BY v HAVING v <= 1 ORDER BY v;
DROP TABLE diff_having_group;
