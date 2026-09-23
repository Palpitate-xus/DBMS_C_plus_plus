-- ALTER TABLE and DML share the outer transaction rollback boundary.
DROP TABLE IF EXISTS pgdiff_alter_txn;
CREATE TABLE pgdiff_alter_txn (a INT);
BEGIN;
ALTER TABLE pgdiff_alter_txn ADD COLUMN b INT;
INSERT INTO pgdiff_alter_txn VALUES (1, 2);
ROLLBACK;
SELECT * FROM pgdiff_alter_txn;
