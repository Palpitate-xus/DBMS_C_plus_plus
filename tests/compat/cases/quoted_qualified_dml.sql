DROP SCHEMA IF EXISTS diff_qqd CASCADE;
CREATE SCHEMA diff_qqd;
CREATE TABLE diff_qqd.items (id integer PRIMARY KEY, v integer);
INSERT INTO "diff_qqd"."items" VALUES (1, 10);
SELECT id, v FROM "diff_qqd"."items";
UPDATE "diff_qqd"."items" SET v = 11 WHERE id = 1;
SELECT id, v FROM diff_qqd.items;
DELETE FROM "diff_qqd"."items" WHERE id = 1;
SELECT id, v FROM diff_qqd.items;
