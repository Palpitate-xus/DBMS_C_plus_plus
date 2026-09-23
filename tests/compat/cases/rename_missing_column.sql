-- RENAME COLUMN reports undefined_column for an absent source column.
DROP TABLE IF EXISTS pgdiff_rename_missing_column;
CREATE TABLE pgdiff_rename_missing_column (a INT);
ALTER TABLE pgdiff_rename_missing_column RENAME COLUMN z TO b;
ALTER TABLE pgdiff_rename_missing_column RENAME COLUMN z TO z;
SELECT a FROM pgdiff_rename_missing_column;
