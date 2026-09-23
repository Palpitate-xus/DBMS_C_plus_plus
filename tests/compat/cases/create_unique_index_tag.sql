-- UNIQUE is a CREATE INDEX option, not part of the command tag.
DROP TABLE IF EXISTS pgdiff_unique_index_tag;
CREATE TABLE pgdiff_unique_index_tag (a INT);
CREATE UNIQUE INDEX pgdiff_unique_tag_idx ON pgdiff_unique_index_tag (a);
SELECT a FROM pgdiff_unique_index_tag;
