-- Ordered table scalar subqueries still need one typed value and NULL bitmap.
CREATE TABLE diff_scalar_ordered(id integer, v text);
INSERT INTO diff_scalar_ordered VALUES (1, 'a b'), (2, 'NULL');
SELECT (SELECT v FROM diff_scalar_ordered ORDER BY id DESC LIMIT 1) AS v;
SELECT (SELECT v FROM diff_scalar_ordered ORDER BY id DESC LIMIT 1 OFFSET 1) AS v;
SELECT (SELECT v FROM diff_scalar_ordered WHERE id = 999 ORDER BY id LIMIT 1) AS v;
DROP TABLE diff_scalar_ordered;
