-- DROP COLUMN without IF EXISTS reports undefined_column.
DROP TABLE IF EXISTS pgdiff_drop_missing_column;
CREATE TABLE pgdiff_drop_missing_column (a INT);
ALTER TABLE pgdiff_drop_missing_column DROP COLUMN z;
SELECT a FROM pgdiff_drop_missing_column;
