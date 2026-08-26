-- mixed plain column and arithmetic expression in SELECT list
DROP TABLE IF EXISTS diff_mx1
CREATE TABLE diff_mx1 (id numeric, g text)
INSERT INTO diff_mx1 VALUES (1, 'a'), (2, 'a'), (3, 'b')
SELECT g, id + 1 FROM diff_mx1 ORDER BY id LIMIT 2
SELECT id + 1, g FROM diff_mx1 ORDER BY id LIMIT 2
SELECT id + 1 FROM diff_mx1 ORDER BY id LIMIT 2
SELECT id + 1, id * 2 FROM diff_mx1 ORDER BY id LIMIT 2
SELECT g, id - 1, id + 10 FROM diff_mx1 WHERE id > 1 ORDER BY id
