-- Multi-table projection, typed protocol metadata and post-outer-join WHERE.
DROP TABLE IF EXISTS pgdiff_multijoin_c;
DROP TABLE IF EXISTS pgdiff_multijoin_b;
DROP TABLE IF EXISTS pgdiff_multijoin_a;
CREATE TABLE pgdiff_multijoin_a(id INT, label TEXT);
CREATE TABLE pgdiff_multijoin_b(id INT, a_id INT, label TEXT);
CREATE TABLE pgdiff_multijoin_c(id INT, b_id INT, label TEXT);
INSERT INTO pgdiff_multijoin_a VALUES (1, 'alpha one'), (2, '');
INSERT INTO pgdiff_multijoin_b VALUES (1, 1, 'literal NULL'), (2, 2, NULL);
INSERT INTO pgdiff_multijoin_c VALUES (1, 1, 'gamma value');
SELECT a.id FROM pgdiff_multijoin_a a LEFT JOIN pgdiff_multijoin_b b ON a.id = b.a_id LEFT JOIN pgdiff_multijoin_c c ON b.id = c.b_id;
SELECT c.label AS result, a.id AS key FROM pgdiff_multijoin_a a LEFT JOIN pgdiff_multijoin_b b ON a.id = b.a_id LEFT JOIN pgdiff_multijoin_c c ON b.id = c.b_id WHERE b.label IS NULL;
SELECT * FROM pgdiff_multijoin_a a LEFT JOIN pgdiff_multijoin_b b ON a.id = b.a_id LEFT JOIN pgdiff_multijoin_c c ON b.id = c.b_id;
