-- A rolled-back DROP restores an UNLOGGED table and its data.
CREATE UNLOGGED TABLE diff_unlogged_drop_txn(v integer);
INSERT INTO diff_unlogged_drop_txn VALUES (7);
BEGIN;
INSERT INTO diff_unlogged_drop_txn VALUES (8);
DROP TABLE diff_unlogged_drop_txn;
ROLLBACK;
SELECT v FROM diff_unlogged_drop_txn;
DROP TABLE diff_unlogged_drop_txn;
