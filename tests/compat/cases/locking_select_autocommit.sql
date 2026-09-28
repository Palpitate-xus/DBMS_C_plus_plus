-- A standalone locking SELECT must use statement-scoped row locks without
-- changing rows, metadata, or command tags relative to PostgreSQL.
CREATE TABLE lock_select_autocommit (id INTEGER PRIMARY KEY);
INSERT INTO lock_select_autocommit VALUES (1);
SELECT id FROM lock_select_autocommit FOR SHARE NOWAIT;
SELECT id FROM lock_select_autocommit FOR UPDATE SKIP LOCKED;
DROP TABLE lock_select_autocommit;
