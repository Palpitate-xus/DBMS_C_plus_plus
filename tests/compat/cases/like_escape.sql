-- LIKE with ESCAPE clause
DROP TABLE IF EXISTS diff_esc1
CREATE TABLE diff_esc1 (v text)
INSERT INTO diff_esc1 VALUES ('a%c'), ('abc'), ('a_c'), (NULL)
SELECT v FROM diff_esc1 WHERE v LIKE 'a!%c' ESCAPE '!'
SELECT v FROM diff_esc1 WHERE v LIKE 'a!_c' ESCAPE '!'
SELECT v FROM diff_esc1 WHERE v LIKE 'a!!%c' ESCAPE '!'
