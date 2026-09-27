-- A rolled-back TRUNCATE restores committed UNLOGGED rows.
CREATE UNLOGGED TABLE diff_unlogged_truncate_txn(v integer);
INSERT INTO diff_unlogged_truncate_txn VALUES (7);
BEGIN;
TRUNCATE TABLE diff_unlogged_truncate_txn;
ROLLBACK;
SELECT v FROM diff_unlogged_truncate_txn;
DROP TABLE diff_unlogged_truncate_txn;
