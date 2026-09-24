DROP TABLE IF EXISTS diff_qrt;
DROP TABLE IF EXISTS "MixedTable";
CREATE TABLE diff_qrt (id integer PRIMARY KEY);
ALTER TABLE diff_qrt RENAME TO "MixedTable";
INSERT INTO "MixedTable" VALUES (1);
SELECT id FROM "MixedTable";
