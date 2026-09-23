-- Constraint names collide in their own namespace (duplicate_object).
DROP TABLE IF EXISTS pgdiff_rename_duplicate_constraint;
CREATE TABLE pgdiff_rename_duplicate_constraint (a INT, CONSTRAINT pgdiff_constraint_a CHECK (a > 0), CONSTRAINT pgdiff_constraint_b CHECK (a < 10));
ALTER TABLE pgdiff_rename_duplicate_constraint RENAME CONSTRAINT pgdiff_constraint_a TO pgdiff_constraint_b;
INSERT INTO pgdiff_rename_duplicate_constraint VALUES (5);
