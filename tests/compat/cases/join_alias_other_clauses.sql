-- Aliases hide original relation names in every query clause.
DROP TABLE IF EXISTS pgdiff_join_scope_other_l;
DROP TABLE IF EXISTS pgdiff_join_scope_other_r;
CREATE TABLE pgdiff_join_scope_other_l (id INT);
CREATE TABLE pgdiff_join_scope_other_r (id INT);
INSERT INTO pgdiff_join_scope_other_l VALUES (2), (1);
INSERT INTO pgdiff_join_scope_other_r VALUES (2), (1);
SELECT l.id FROM pgdiff_join_scope_other_l l JOIN pgdiff_join_scope_other_r r ON l.id = r.id WHERE pgdiff_join_scope_other_l.id = 1;
SELECT count(pgdiff_join_scope_other_l.id) FROM pgdiff_join_scope_other_l l JOIN pgdiff_join_scope_other_r r ON l.id = r.id;
SELECT l.id FROM pgdiff_join_scope_other_l l JOIN pgdiff_join_scope_other_r r ON l.id = r.id ORDER BY pgdiff_join_scope_other_l.id;
SELECT l.id FROM pgdiff_join_scope_other_l l JOIN pgdiff_join_scope_other_r r ON l.id = r.id WHERE l.id = 1;
SELECT count(l.id) FROM pgdiff_join_scope_other_l l JOIN pgdiff_join_scope_other_r r ON l.id = r.id;
SELECT l.id FROM pgdiff_join_scope_other_l l JOIN pgdiff_join_scope_other_r r ON l.id = r.id ORDER BY l.id;
