-- Index names occupy the relation namespace and use duplicate_table.
DROP TABLE IF EXISTS pgdiff_create_duplicate_index;
CREATE TABLE pgdiff_create_duplicate_index (a INT);
CREATE INDEX pgdiff_duplicate_idx ON pgdiff_create_duplicate_index (a);
CREATE INDEX pgdiff_duplicate_idx ON pgdiff_create_duplicate_index (a);
SELECT a FROM pgdiff_create_duplicate_index;
