-- left, right, repeat string functions
DROP TABLE IF EXISTS diff_lr1
CREATE TABLE diff_lr1 (v text)
INSERT INTO diff_lr1 VALUES ('hello'), ('abc')
SELECT left(v, 3), right(v, 3) FROM diff_lr1
SELECT left(v, -2), right(v, -2) FROM diff_lr1
SELECT repeat(v, 2) FROM diff_lr1
SELECT left('hello', 0), right('hello', 10), repeat('ab', 3) FROM diff_lr1
