-- string concatenation operator ||
DROP TABLE IF EXISTS diff_cc1
CREATE TABLE diff_cc1 (id numeric, g text)
INSERT INTO diff_cc1 VALUES (1, 'a'), (2, 'a'), (3, 'b')
SELECT 'x' || 'y' AS lit
SELECT g || g FROM diff_cc1 ORDER BY id LIMIT 2
SELECT id::text || g FROM diff_cc1 ORDER BY id LIMIT 3
SELECT upper(g) || '!' FROM diff_cc1 ORDER BY id LIMIT 2
SELECT g || '-' || g FROM diff_cc1 WHERE id = 3
