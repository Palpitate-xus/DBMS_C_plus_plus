-- Additional ON conjuncts are part of matching, not post-join WHERE.
DROP TABLE IF EXISTS pgdiff_join_on_a;
DROP TABLE IF EXISTS pgdiff_join_on_b;
CREATE TABLE pgdiff_join_on_a (id INT);
CREATE TABLE pgdiff_join_on_b (id INT, v INT);
INSERT INTO pgdiff_join_on_a VALUES (7), (8);
INSERT INTO pgdiff_join_on_b VALUES (7, 0), (8, 1);
SELECT a.id, b.v FROM pgdiff_join_on_a a JOIN pgdiff_join_on_b b ON a.id = b.id AND b.v > 0;
SELECT a.id, b.v FROM pgdiff_join_on_a a JOIN pgdiff_join_on_b b ON b.v > 0 AND a.id = b.id;
SELECT a.id, b.v FROM pgdiff_join_on_a a JOIN pgdiff_join_on_b b ON a.id = b.id AND a.id > b.v;
SELECT a.id, b.v FROM pgdiff_join_on_a a LEFT JOIN pgdiff_join_on_b b ON a.id = b.id AND b.v > 0 ORDER BY a.id;
SELECT a.id, b.v FROM pgdiff_join_on_a a LEFT JOIN pgdiff_join_on_b b ON a.id = b.id AND b.v > 0 WHERE b.v IS NULL;
SELECT a.id, b.v FROM pgdiff_join_on_a a RIGHT JOIN pgdiff_join_on_b b ON a.id = b.id AND b.v > 0 ORDER BY b.id;
SELECT a.id, b.v FROM pgdiff_join_on_a a FULL OUTER JOIN pgdiff_join_on_b b ON a.id = b.id AND b.v > 0 ORDER BY a.id NULLS LAST, b.v NULLS LAST;
