-- Quoted owning columns must be bound and survive sequence-file reload.
DROP SCHEMA IF EXISTS diff_seq_col CASCADE;
CREATE SCHEMA diff_seq_col;
CREATE TABLE diff_seq_col.owner_table ("id space" bigint, "id.dot" bigint);
CREATE SEQUENCE diff_seq_col.owned_seq OWNED BY diff_seq_col.owner_table."id space";
CREATE SEQUENCE diff_seq_col.dot_seq;
ALTER SEQUENCE diff_seq_col.dot_seq OWNED BY diff_seq_col.owner_table."id.dot";
SELECT nextval('diff_seq_col.owned_seq');
SELECT nextval('diff_seq_col.dot_seq');
DROP TABLE diff_seq_col.owner_table;
SELECT nextval('diff_seq_col.owned_seq');
SELECT nextval('diff_seq_col.dot_seq');
DROP SCHEMA diff_seq_col;
