-- Rolling back to a savepoint restores an UNLOGGED table and its rows.
CREATE UNLOGGED TABLE diff_unlogged_savepoint(v integer);
INSERT INTO diff_unlogged_savepoint VALUES (7);
BEGIN;
SAVEPOINT before_drop;
DROP TABLE diff_unlogged_savepoint;
ROLLBACK TO SAVEPOINT before_drop;
SELECT v FROM diff_unlogged_savepoint;
COMMIT;
DROP TABLE diff_unlogged_savepoint;
