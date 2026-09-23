-- Tables and indexes share a relation namespace in PostgreSQL.
DROP TABLE IF EXISTS pgdiff_rename_to_index_collision;
CREATE TABLE pgdiff_rename_to_index_collision (a INT);
CREATE INDEX pgdiff_rename_to_index_target ON pgdiff_rename_to_index_collision (a);
ALTER TABLE pgdiff_rename_to_index_collision RENAME TO pgdiff_rename_to_index_target;
SELECT a FROM pgdiff_rename_to_index_collision;
