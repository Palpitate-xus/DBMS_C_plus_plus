-- IS [NOT] NULL belongs to ON matching, before outer-row extension.
DROP TABLE IF EXISTS pgdiff_join_null_on_l;
DROP TABLE IF EXISTS pgdiff_join_null_on_r;
CREATE TABLE pgdiff_join_null_on_l (id INT);
CREATE TABLE pgdiff_join_null_on_r (id INT, v INT);
INSERT INTO pgdiff_join_null_on_l VALUES (1), (2), (3);
INSERT INTO pgdiff_join_null_on_r VALUES (1, NULL), (2, 5), (4, NULL);
SELECT l.id, r.id, r.v FROM pgdiff_join_null_on_l l JOIN pgdiff_join_null_on_r r ON l.id = r.id AND r.v IS NULL ORDER BY l.id;
SELECT l.id, r.id, r.v FROM pgdiff_join_null_on_l l LEFT JOIN pgdiff_join_null_on_r r ON l.id = r.id AND r.v IS NULL ORDER BY l.id;
SELECT l.id, r.id, r.v FROM pgdiff_join_null_on_l l RIGHT JOIN pgdiff_join_null_on_r r ON r.v IS NOT NULL AND l.id = r.id ORDER BY r.id;
SELECT l.id, r.id, r.v FROM pgdiff_join_null_on_l l FULL OUTER JOIN pgdiff_join_null_on_r r ON l.id = r.id AND r.v IS NULL ORDER BY l.id NULLS LAST, r.id NULLS LAST;
SELECT l.id, r.id FROM pgdiff_join_null_on_l l LEFT JOIN pgdiff_join_null_on_r r ON r.v IS NULL ORDER BY l.id, r.id NULLS LAST;
