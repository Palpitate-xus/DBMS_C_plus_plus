DROP SCHEMA IF EXISTS "Diff.Schema" CASCADE;
CREATE SCHEMA "Diff.Schema";
CREATE TABLE "Diff.Schema".items (id integer);
INSERT INTO "Diff.Schema".items VALUES (5);
SELECT id FROM "Diff.Schema".items;
DROP SCHEMA "Diff.Schema" CASCADE;
SELECT id FROM "Diff.Schema".items;
