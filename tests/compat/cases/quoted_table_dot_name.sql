DROP TABLE IF EXISTS "Diff.Quote";
CREATE TABLE "Diff.Quote" (id integer);
INSERT INTO "Diff.Quote" VALUES (3);
SELECT id FROM "Diff.Quote";
UPDATE "Diff.Quote" SET id = 4 WHERE id = 3;
SELECT id FROM "Diff.Quote";
DELETE FROM "Diff.Quote" WHERE id = 4;
SELECT id FROM "Diff.Quote";
