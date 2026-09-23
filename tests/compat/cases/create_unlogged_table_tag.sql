-- UNLOGGED changes persistence, not the CREATE TABLE command tag.
DROP TABLE IF EXISTS pgdiff_unlogged_tag;
CREATE UNLOGGED TABLE pgdiff_unlogged_tag(a INT);
INSERT INTO pgdiff_unlogged_tag VALUES (1);
SELECT a FROM pgdiff_unlogged_tag;
DROP TABLE pgdiff_unlogged_tag;
