DROP TABLE IF EXISTS diff_drop_sequence_kind;
CREATE TABLE diff_drop_sequence_kind (id integer);
INSERT INTO diff_drop_sequence_kind VALUES (59);
DROP SEQUENCE diff_drop_sequence_kind;
DROP SEQUENCE IF EXISTS diff_drop_sequence_kind;
SELECT id FROM diff_drop_sequence_kind;
DROP TABLE diff_drop_sequence_kind;
