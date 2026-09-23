-- Once a relation has an alias, its original name is not visible to ON.
DROP TABLE IF EXISTS pgdiff_join_alias_scope_l;
DROP TABLE IF EXISTS pgdiff_join_alias_scope_r;
CREATE TABLE pgdiff_join_alias_scope_l (id INT);
CREATE TABLE pgdiff_join_alias_scope_r (id INT);
INSERT INTO pgdiff_join_alias_scope_l VALUES (1);
INSERT INTO pgdiff_join_alias_scope_r VALUES (1);
SELECT l.id FROM pgdiff_join_alias_scope_l l JOIN pgdiff_join_alias_scope_r r ON pgdiff_join_alias_scope_l.id = r.id;
SELECT l.id FROM pgdiff_join_alias_scope_l l JOIN pgdiff_join_alias_scope_r r ON l.id = pgdiff_join_alias_scope_r.id;
SELECT pgdiff_join_alias_scope_l.id FROM pgdiff_join_alias_scope_l l JOIN pgdiff_join_alias_scope_r r ON l.id = r.id;
SELECT l.id FROM pgdiff_join_alias_scope_l l JOIN pgdiff_join_alias_scope_r r ON r.id = l.id;
SELECT pgdiff_join_alias_scope_l.id FROM pgdiff_join_alias_scope_l JOIN pgdiff_join_alias_scope_r ON pgdiff_join_alias_scope_l.id = pgdiff_join_alias_scope_r.id;
