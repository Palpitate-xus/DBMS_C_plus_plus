DROP SCHEMA IF EXISTS "Diff.Drop.Schema" CASCADE;
CREATE SCHEMA "Diff.Drop.Schema";
CREATE TABLE "Diff.Drop.Schema"."t.with.dot" (id integer);
INSERT INTO "Diff.Drop.Schema"."t.with.dot" VALUES (7);
DROP TABLE "Diff.Drop.Schema"."t.with.dot";
DROP SCHEMA "Diff.Drop.Schema";
