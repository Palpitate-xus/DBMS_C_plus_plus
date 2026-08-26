-- set operations: INTERSECT / EXCEPT / UNION
-- (unordered set-op output is implementation-defined; every statement
--  below pins the order with ORDER BY)
DROP TABLE IF EXISTS diff_so1
DROP TABLE IF EXISTS diff_so2
CREATE TABLE diff_so1 (v numeric)
CREATE TABLE diff_so2 (v numeric)
INSERT INTO diff_so1 VALUES (1), (2), (2), (3)
INSERT INTO diff_so2 VALUES (2), (2), (4)
SELECT v FROM diff_so1 INTERSECT SELECT v FROM diff_so2 ORDER BY v
SELECT v FROM diff_so1 INTERSECT SELECT v FROM diff_so2 ORDER BY v DESC
SELECT v FROM diff_so1 EXCEPT SELECT v FROM diff_so2 ORDER BY v
SELECT v FROM diff_so1 UNION SELECT v FROM diff_so2 ORDER BY v DESC
SELECT v FROM diff_so1 UNION ALL SELECT v FROM diff_so2 ORDER BY v
SELECT v FROM diff_so1 EXCEPT SELECT v FROM diff_so2 ORDER BY v DESC LIMIT 1
