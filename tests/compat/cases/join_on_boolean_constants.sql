-- Constant ON predicates control matching; outer rows survive false matches.
DROP TABLE IF EXISTS pgdiff_join_bool_on_l;
DROP TABLE IF EXISTS pgdiff_join_bool_on_r;
CREATE TABLE pgdiff_join_bool_on_l (id INT);
CREATE TABLE pgdiff_join_bool_on_r (id INT);
INSERT INTO pgdiff_join_bool_on_l VALUES (1), (2);
INSERT INTO pgdiff_join_bool_on_r VALUES (3);
SELECT l.id, r.id FROM pgdiff_join_bool_on_l l JOIN pgdiff_join_bool_on_r r ON TRUE ORDER BY l.id;
SELECT l.id, r.id FROM pgdiff_join_bool_on_l l JOIN pgdiff_join_bool_on_r r ON FALSE ORDER BY l.id;
SELECT l.id, r.id FROM pgdiff_join_bool_on_l l LEFT JOIN pgdiff_join_bool_on_r r ON TRUE ORDER BY l.id;
SELECT l.id, r.id FROM pgdiff_join_bool_on_l l LEFT JOIN pgdiff_join_bool_on_r r ON FALSE ORDER BY l.id;
SELECT l.id, r.id FROM pgdiff_join_bool_on_l l RIGHT JOIN pgdiff_join_bool_on_r r ON FALSE;
SELECT l.id, r.id FROM pgdiff_join_bool_on_l l LEFT JOIN pgdiff_join_bool_on_r r ON TRUE AND FALSE ORDER BY l.id;
SELECT l.id, r.id FROM pgdiff_join_bool_on_l l FULL OUTER JOIN pgdiff_join_bool_on_r r ON TRUE;
