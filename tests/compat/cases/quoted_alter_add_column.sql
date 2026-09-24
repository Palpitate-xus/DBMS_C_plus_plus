DROP TABLE IF EXISTS diff_qac;
CREATE TABLE diff_qac (id integer PRIMARY KEY);
ALTER TABLE diff_qac ADD COLUMN "MixedValue" integer;
INSERT INTO diff_qac (id, "MixedValue") VALUES (1, 10);
SELECT id, "MixedValue" FROM diff_qac;
