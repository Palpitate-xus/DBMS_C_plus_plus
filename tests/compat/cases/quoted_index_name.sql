DROP TABLE IF EXISTS diff_qidx;
CREATE TABLE diff_qidx (id integer, "MixedValue" integer);
CREATE INDEX "Mixed Idx" ON diff_qidx ("MixedValue");
DROP INDEX "Mixed Idx";
CREATE INDEX "Mixed Idx" ON diff_qidx ("MixedValue");
CREATE INDEX "IncludeIdx" ON diff_qidx (id) INCLUDE ("MixedValue");
DROP INDEX "IncludeIdx";
INSERT INTO diff_qidx VALUES (1, 2);
SELECT "MixedValue" FROM diff_qidx WHERE "MixedValue" = 2;
