DROP TABLE IF EXISTS diff_qrc;
CREATE TABLE diff_qrc (id integer, CONSTRAINT "MixedCheck" CHECK (id > 0));
INSERT INTO diff_qrc VALUES (1);
ALTER TABLE diff_qrc RENAME CONSTRAINT "MixedCheck" TO "NewCheck";
ALTER TABLE diff_qrc DROP CONSTRAINT "NewCheck";
ALTER TABLE diff_qrc ADD CONSTRAINT "ExtraCheck" CHECK (id > -10);
ALTER TABLE diff_qrc DROP CONSTRAINT "ExtraCheck";
INSERT INTO diff_qrc VALUES (-1);
SELECT id FROM diff_qrc ORDER BY id;
