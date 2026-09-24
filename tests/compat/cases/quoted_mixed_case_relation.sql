DROP SCHEMA IF EXISTS diff_qmcr CASCADE;
CREATE SCHEMA diff_qmcr;
CREATE TABLE diff_qmcr."MixedItems" (id integer PRIMARY KEY, v integer);
INSERT INTO diff_qmcr."MixedItems" VALUES (1, 10);
SELECT id, v FROM diff_qmcr."MixedItems";
UPDATE diff_qmcr."MixedItems" SET v = 11 WHERE id = 1;
SELECT id, v FROM diff_qmcr."MixedItems";
DELETE FROM diff_qmcr."MixedItems" WHERE id = 1;
SELECT id, v FROM diff_qmcr."MixedItems";
