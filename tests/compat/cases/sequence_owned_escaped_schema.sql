-- Embedded double quotes in an owning schema retain identifier boundaries.
DROP SCHEMA IF EXISTS "Diff""Owned" CASCADE;
CREATE SCHEMA "Diff""Owned";
CREATE TABLE "Diff""Owned".owner_table (id bigint);
CREATE SEQUENCE "Diff""Owned".owned_seq OWNED BY "Diff""Owned".owner_table.id;
SELECT nextval('"Diff""Owned".owned_seq');
DROP TABLE "Diff""Owned".owner_table;
SELECT nextval('"Diff""Owned".owned_seq');
DROP SCHEMA "Diff""Owned";
