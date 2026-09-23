-- Renaming a table to an existing relation reports duplicate_table.
DROP TABLE IF EXISTS pgdiff_rename_duplicate_table_a;
DROP TABLE IF EXISTS pgdiff_rename_duplicate_table_b;
CREATE TABLE pgdiff_rename_duplicate_table_a (a INT);
CREATE TABLE pgdiff_rename_duplicate_table_b (b INT);
ALTER TABLE pgdiff_rename_duplicate_table_a RENAME TO pgdiff_rename_duplicate_table_b;
SELECT a FROM pgdiff_rename_duplicate_table_a;
