-- Transactional DROP must restore UNLOGGED heap and TOAST contents.
CREATE UNLOGGED TABLE diff_unlogged_toast_drop(id integer, payload text);
INSERT INTO diff_unlogged_toast_drop VALUES (7, repeat('x', 5000));
CREATE UNIQUE INDEX diff_unlogged_toast_drop_id_idx ON diff_unlogged_toast_drop(id);
BEGIN;
DROP TABLE diff_unlogged_toast_drop;
ROLLBACK;
SELECT id, length(payload), substring(payload FROM 4998 FOR 3) FROM diff_unlogged_toast_drop;
INSERT INTO diff_unlogged_toast_drop VALUES (7, 'duplicate');
DROP TABLE diff_unlogged_toast_drop;
