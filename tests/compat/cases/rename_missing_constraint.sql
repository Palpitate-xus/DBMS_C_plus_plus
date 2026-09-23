-- RENAME CONSTRAINT reports undefined_object for an absent source.
DROP TABLE IF EXISTS pgdiff_rename_missing_constraint;
CREATE TABLE pgdiff_rename_missing_constraint (a INT);
ALTER TABLE pgdiff_rename_missing_constraint RENAME CONSTRAINT z TO b;
ALTER TABLE pgdiff_rename_missing_constraint RENAME CONSTRAINT z TO z;
SELECT a FROM pgdiff_rename_missing_constraint;
