SELECT 1 /* open
BEGIN;
SELECT 1 /* outer /* inner */
SELECT 1;
ROLLBACK;
BEGIN;
SAVEPOINT child;
SELECT 1 /* open
ROLLBACK TO child;
SELECT 2;
ROLLBACK;
SELECT '/* literal */' AS data;
