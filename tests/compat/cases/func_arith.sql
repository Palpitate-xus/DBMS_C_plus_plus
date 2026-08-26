-- function call combined with arithmetic in SELECT list
DROP TABLE IF EXISTS diff_fa1
CREATE TABLE diff_fa1 (a numeric, b numeric)
INSERT INTO diff_fa1 VALUES (1, NULL), (NULL, 2), (NULL, NULL)
SELECT coalesce(a, b) + 1 FROM diff_fa1
SELECT coalesce(a, b, 0) * 2 FROM diff_fa1
SELECT greatest(a, b, 5) - 1 FROM diff_fa1
SELECT nullif(a, 99) + 10 FROM diff_fa1
