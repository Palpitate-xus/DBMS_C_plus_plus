-- negative integers: storage, modulo, integer division
DROP TABLE IF EXISTS diff_neg1
CREATE TABLE diff_neg1 (a int, b int)
INSERT INTO diff_neg1 VALUES (-7, 3), (7, -3), (-7, -3)
SELECT a, b FROM diff_neg1
SELECT a % b FROM diff_neg1
SELECT a / b FROM diff_neg1
SELECT 7 / 2, -7 / 2, 7 / -2, -7 / -2 FROM diff_neg1
SELECT -a FROM diff_neg1
