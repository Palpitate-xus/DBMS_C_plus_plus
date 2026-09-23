-- ADD COLUMN duplicate names use duplicate_column, not an internal error.
DROP TABLE IF EXISTS pgdiff_duplicate_column;
CREATE TABLE pgdiff_duplicate_column (a INT);
ALTER TABLE pgdiff_duplicate_column ADD COLUMN a INT;
SELECT a FROM pgdiff_duplicate_column;
