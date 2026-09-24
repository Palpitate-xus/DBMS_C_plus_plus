DROP TABLE IF EXISTS diff_qcd;
CREATE TABLE diff_qcd (id integer PRIMARY KEY, "MixedValue" integer, "current_user" text);
INSERT INTO diff_qcd (id, "MixedValue", "current_user") VALUES (1, 10, 'stored');
SELECT "MixedValue" FROM diff_qcd WHERE id = 1;
SELECT "current_user" FROM diff_qcd WHERE id = 1;
SELECT id FROM diff_qcd WHERE "MixedValue" = 10;
UPDATE diff_qcd SET "MixedValue" = 11 WHERE id = 1;
SELECT id, "MixedValue" FROM diff_qcd;
SELECT id FROM diff_qcd WHERE "MixedValue" = 11 ORDER BY "MixedValue";
