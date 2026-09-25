DROP TABLE IF EXISTS diff_drop_index_kind;
CREATE TABLE diff_drop_index_kind (id integer);
INSERT INTO diff_drop_index_kind VALUES (43);
DROP INDEX diff_drop_index_kind;
DROP INDEX IF EXISTS diff_drop_index_kind;
SELECT id FROM diff_drop_index_kind;
DROP TABLE diff_drop_index_kind;
