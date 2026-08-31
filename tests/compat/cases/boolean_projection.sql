DROP TABLE IF EXISTS diff_bool;
CREATE TABLE diff_bool (a INT, b INT);
INSERT INTO diff_bool VALUES (1, 1), (1, 2), (1, NULL), (NULL, NULL);
SELECT a = b or a is null FROM diff_bool;
SELECT a is null or b is null FROM diff_bool;
SELECT a > 0 and b > 0 FROM diff_bool;
SELECT a = 1 or b = 2 FROM diff_bool;
SELECT a IS DISTINCT FROM b FROM diff_bool;
