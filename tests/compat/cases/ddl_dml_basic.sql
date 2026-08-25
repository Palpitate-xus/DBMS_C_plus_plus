-- stateful DDL/DML: create, insert, project, aggregate
DROP TABLE IF EXISTS diff_t1
CREATE TABLE diff_t1 (a int, b text)
INSERT INTO diff_t1 VALUES (1, 'x'), (2, 'y'), (3, 'z')
SELECT a, b FROM diff_t1 ORDER BY a
SELECT count(*) FROM diff_t1
SELECT b FROM diff_t1 WHERE a > 1 ORDER BY b
SELECT a + a FROM diff_t1 ORDER BY 1
SELECT max(a), min(a) FROM diff_t1
UPDATE diff_t1 SET b = 'w' WHERE a = 2
SELECT b FROM diff_t1 ORDER BY a
DELETE FROM diff_t1 WHERE a = 3
SELECT count(*) FROM diff_t1
