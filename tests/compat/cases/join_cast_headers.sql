-- Bare-column casts in JOIN projections retain the source column name.
DROP TABLE IF EXISTS pgdiff_join_cast_a;
DROP TABLE IF EXISTS pgdiff_join_cast_b;
CREATE TABLE pgdiff_join_cast_a (id INT);
CREATE TABLE pgdiff_join_cast_b (id INT);
INSERT INTO pgdiff_join_cast_a VALUES (7);
INSERT INTO pgdiff_join_cast_b VALUES (7);
SELECT a.id::text, CAST(b.id AS text) FROM pgdiff_join_cast_a a JOIN pgdiff_join_cast_b b ON a.id = b.id;
SELECT (a.id + 1)::text, CAST(b.id + 1 AS text), 1::int, 'x'::varchar FROM pgdiff_join_cast_a a JOIN pgdiff_join_cast_b b ON a.id = b.id;
SELECT CAST(a.id AS text) AS converted, b.id FROM pgdiff_join_cast_a a JOIN pgdiff_join_cast_b b ON a.id = b.id ORDER BY converted;
SELECT a.id::int + 1 FROM pgdiff_join_cast_a a JOIN pgdiff_join_cast_b b ON a.id = b.id;
