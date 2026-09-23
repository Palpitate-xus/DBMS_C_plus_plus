-- Scalar JOIN projection names and types beyond functions, CAST and CASE.
DROP TABLE IF EXISTS pgdiff_join_scalar_a;
DROP TABLE IF EXISTS pgdiff_join_scalar_b;
CREATE TABLE pgdiff_join_scalar_a (id INT);
CREATE TABLE pgdiff_join_scalar_b (id INT);
INSERT INTO pgdiff_join_scalar_a VALUES (7);
INSERT INTO pgdiff_join_scalar_b VALUES (7);
SELECT a.id + b.id, -a.id, 1, NULL, a.id IS NULL, a.id = b.id FROM pgdiff_join_scalar_a a JOIN pgdiff_join_scalar_b b ON a.id = b.id;
SELECT a.id + b.id, -a.id, 1, NULL, a.id IS NULL, a.id = b.id FROM pgdiff_join_scalar_a a JOIN pgdiff_join_scalar_b b ON a.id = b.id WHERE a.id < 0;
SELECT a.id + b.id, COALESCE(b.id, 0), b.id IS NULL FROM pgdiff_join_scalar_a a LEFT JOIN pgdiff_join_scalar_b b ON a.id < 0;
INSERT INTO pgdiff_join_scalar_b VALUES (9);
SELECT a.id, b.id FROM pgdiff_join_scalar_a a JOIN pgdiff_join_scalar_b b ON a.id < b.id;
SELECT a.id, b.id FROM pgdiff_join_scalar_a a JOIN pgdiff_join_scalar_b b ON a.id <> b.id;
SELECT a.id, b.id FROM pgdiff_join_scalar_a a JOIN pgdiff_join_scalar_b b ON 0 < a.id;
SELECT a.id, b.id FROM pgdiff_join_scalar_a a LEFT JOIN pgdiff_join_scalar_b b ON a.id = NULL;
SELECT a.id, b.id FROM pgdiff_join_scalar_a a LEFT JOIN pgdiff_join_scalar_b b ON a.id < 0 WHERE b.id IS NULL;
SELECT a.id, b.id FROM pgdiff_join_scalar_a a RIGHT JOIN pgdiff_join_scalar_b b ON a.id < 0 ORDER BY 2;
SELECT a.id, b.id FROM pgdiff_join_scalar_a a FULL OUTER JOIN pgdiff_join_scalar_b b ON a.id < 0 ORDER BY 2 NULLS LAST;
