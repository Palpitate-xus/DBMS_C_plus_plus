DROP TABLE IF EXISTS "DiffQIT";
CREATE TABLE "DiffQIT" ("MixedValue" integer);
CREATE INDEX diff_qit_idx ON "DiffQIT" ("MixedValue");
INSERT INTO "DiffQIT" VALUES (7);
SELECT "MixedValue" FROM "DiffQIT" WHERE "MixedValue" = 7;
DROP INDEX diff_qit_idx;
