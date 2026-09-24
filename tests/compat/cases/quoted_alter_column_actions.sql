DROP TABLE IF EXISTS diff_qca;
CREATE TABLE diff_qca (id integer PRIMARY KEY, "MixedValue" integer);
ALTER TABLE diff_qca ALTER COLUMN "MixedValue" SET DEFAULT 7;
INSERT INTO diff_qca (id) VALUES (1);
SELECT id, "MixedValue" FROM diff_qca;
ALTER TABLE diff_qca ALTER COLUMN "MixedValue" SET NOT NULL;
ALTER TABLE diff_qca DROP COLUMN "MixedValue";
SELECT id FROM diff_qca;
