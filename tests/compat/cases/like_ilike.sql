-- LIKE / ILIKE case sensitivity and negation
DROP TABLE IF EXISTS diff_lk1
CREATE TABLE diff_lk1 (v text)
INSERT INTO diff_lk1 VALUES ('abc'), ('ABC'), ('abd'), ('a_c'), ('xyz'), (NULL)
SELECT v FROM diff_lk1 WHERE v LIKE 'ab%'
SELECT v FROM diff_lk1 WHERE v ILIKE 'ab%'
SELECT v FROM diff_lk1 WHERE v NOT LIKE 'a_c'
SELECT v FROM diff_lk1 WHERE v NOT ILIKE 'a_c'
SELECT v FROM diff_lk1 WHERE v LIKE 'a_c'
SELECT v FROM diff_lk1 WHERE v ILIKE 'A_C'
