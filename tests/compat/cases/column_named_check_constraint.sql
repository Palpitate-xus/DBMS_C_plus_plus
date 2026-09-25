DROP TABLE IF EXISTS diff_qcc;
CREATE TABLE diff_qcc (id integer CONSTRAINT "MixedCheck" CHECK (id > 0));
INSERT INTO diff_qcc VALUES (1);
INSERT INTO diff_qcc VALUES (-1);
UPDATE diff_qcc SET id = -1 WHERE id = 1;
ALTER TABLE diff_qcc DROP CONSTRAINT "MixedCheck";
INSERT INTO diff_qcc VALUES (-1);
SELECT id FROM diff_qcc ORDER BY id;
