-- JOIN expressions must evaluate against each side's typed cells, not a missing column.
DROP TABLE IF EXISTS pgdiff_join_empty_a;
DROP TABLE IF EXISTS pgdiff_join_empty_b;
CREATE TABLE pgdiff_join_empty_a(k INT, v TEXT);
CREATE TABLE pgdiff_join_empty_b(k INT, v TEXT);
INSERT INTO pgdiff_join_empty_a VALUES (1, ''), (2, NULL), (3, 'hello world');
INSERT INTO pgdiff_join_empty_b VALUES (1, ''), (3, 'with spaces');
SELECT a.k, a.v, b.v FROM pgdiff_join_empty_a a LEFT JOIN pgdiff_join_empty_b b ON a.k = b.k ORDER BY a.k;
SELECT a.k, a.v || b.v AS joined FROM pgdiff_join_empty_a a LEFT JOIN pgdiff_join_empty_b b ON a.k = b.k ORDER BY a.k;
SELECT a.k, COALESCE(a.v, 'nil') AS av, COALESCE(b.v, 'nil') AS bv FROM pgdiff_join_empty_a a LEFT JOIN pgdiff_join_empty_b b ON a.k = b.k ORDER BY a.k;
SELECT COALESCE(v, 'nil') FROM pgdiff_join_empty_a a LEFT JOIN pgdiff_join_empty_b b ON a.k = b.k;
SELECT COALESCE(z.v, 'nil') FROM pgdiff_join_empty_a a LEFT JOIN pgdiff_join_empty_b b ON a.k = b.k;
SELECT COALESCE(a.missing_v, 'nil') FROM pgdiff_join_empty_a a LEFT JOIN pgdiff_join_empty_b b ON a.k = b.k;
