-- Test CTE with INSERT/UPDATE/DELETE

USE DATABASE testdb;

-- Test DELETE with RETURNING in CTE
WITH deleted_rows AS (
    DELETE FROM t WHERE a = 1 RETURNING *
)
SELECT * FROM deleted_rows;

-- Check remaining rows
SELECT * FROM t;

-- Test INSERT with RETURNING in CTE
WITH inserted_rows AS (
    INSERT INTO t VALUES (100)
    RETURNING *
)
SELECT * FROM inserted_rows;

-- Test UPDATE with RETURNING in CTE
WITH updated_rows AS (
    UPDATE t SET a = 200 WHERE a = 100 RETURNING *
)
SELECT * FROM updated_rows;

-- Cleanup
DELETE FROM t WHERE a >= 100;
INSERT INTO t VALUES (1);
