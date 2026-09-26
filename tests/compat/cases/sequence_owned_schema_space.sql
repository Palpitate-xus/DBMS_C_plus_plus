-- Quoted whitespace in an owning schema must survive sequence-file reload.
DROP SCHEMA IF EXISTS "Diff Seq Space" CASCADE;
CREATE SCHEMA "Diff Seq Space";
CREATE TABLE "Diff Seq Space".owner_table (id bigint);
CREATE SEQUENCE "Diff Seq Space".owned_seq OWNED BY "Diff Seq Space".owner_table.id;
SELECT nextval('"Diff Seq Space".owned_seq');
DROP TABLE "Diff Seq Space".owner_table;
SELECT nextval('"Diff Seq Space".owned_seq');
DROP SCHEMA "Diff Seq Space";
