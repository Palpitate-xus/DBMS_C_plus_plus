-- NULL ordering (PG: NULLS LAST for ASC, NULLS FIRST for DESC)
DROP TABLE IF EXISTS diff_no
CREATE TABLE diff_no (k numeric, t text)
INSERT INTO diff_no VALUES (1, 'a'), (NULL, 'b'), (3, 'c'), (NULL, 'd')
SELECT k, t FROM diff_no ORDER BY k
SELECT k, t FROM diff_no ORDER BY k DESC
SELECT t, k FROM diff_no ORDER BY t
SELECT k AS kk, t FROM diff_no ORDER BY kk
