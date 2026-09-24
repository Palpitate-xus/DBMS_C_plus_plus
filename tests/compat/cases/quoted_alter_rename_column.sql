DROP TABLE IF EXISTS diff_qrc;
CREATE TABLE diff_qrc (id integer PRIMARY KEY, "MixedValue" integer);
INSERT INTO diff_qrc (id, "MixedValue") VALUES (1, 10);
ALTER TABLE diff_qrc RENAME COLUMN "MixedValue" TO "NewValue";
SELECT id, "NewValue" FROM diff_qrc;
UPDATE diff_qrc SET "NewValue" = 11 WHERE id = 1;
SELECT "NewValue" FROM diff_qrc;
