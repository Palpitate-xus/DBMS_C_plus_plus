-- RENAME COLUMN duplicate names use duplicate_column, not an internal error.
DROP TABLE IF EXISTS pgdiff_rename_duplicate_column;
CREATE TABLE pgdiff_rename_duplicate_column (a INT, b INT);
ALTER TABLE pgdiff_rename_duplicate_column RENAME COLUMN a TO b;
SELECT a, b FROM pgdiff_rename_duplicate_column;
