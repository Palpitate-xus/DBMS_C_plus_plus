-- A rolled-back UNLOGGED table creation must leave the name available.
BEGIN;
CREATE UNLOGGED TABLE diff_unlogged_txn(v integer);
ROLLBACK;
CREATE TABLE diff_unlogged_txn(v integer);
DROP TABLE diff_unlogged_txn;
