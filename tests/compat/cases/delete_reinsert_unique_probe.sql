-- A transaction may reuse both a composite primary key and a distinct UNIQUE key after deleting their old rows.
DROP TABLE IF EXISTS delete_reinsert_unique_probe;
CREATE TABLE delete_reinsert_unique_probe (a INTEGER, b INTEGER, email TEXT UNIQUE, payload TEXT, PRIMARY KEY (a,b));
INSERT INTO delete_reinsert_unique_probe VALUES (1,2,'first@example','old'),(3,4,'second@example','keep');
BEGIN;
DELETE FROM delete_reinsert_unique_probe WHERE a = 1 AND b = 2;
DELETE FROM delete_reinsert_unique_probe WHERE email = 'second@example';
INSERT INTO delete_reinsert_unique_probe VALUES (1,2,'second@example','replacement');
SELECT a,b,email,payload FROM delete_reinsert_unique_probe;
COMMIT;
DROP TABLE delete_reinsert_unique_probe;
