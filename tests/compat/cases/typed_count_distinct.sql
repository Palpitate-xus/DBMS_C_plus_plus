-- Equality follows the input type, not its display scale or float rounding.
DROP TABLE IF EXISTS diff_typed_count;
CREATE TABLE diff_typed_count(g INT,v NUMERIC,t TEXT);
INSERT INTO diff_typed_count VALUES (1,0.50,'0.50'),(1,0.500,'0.500'),(2,2.00,''),(2,2.000,'NULL'),(1,NULL,NULL),(2,NULL,NULL);
SELECT count(DISTINCT v),count(v),count(*) FROM diff_typed_count;
SELECT count(DISTINCT t) FROM diff_typed_count;
SELECT g,count(DISTINCT v) FROM diff_typed_count GROUP BY g ORDER BY g;
SELECT count(DISTINCT v) FROM diff_typed_count WHERE g=1;
SELECT count(DISTINCT v) FILTER (WHERE g=2) FROM diff_typed_count;
SELECT count(DISTINCT v) FROM diff_typed_count WHERE g=9;
DROP TABLE diff_typed_count;
CREATE TABLE diff_typed_count(v NUMERIC);
INSERT INTO diff_typed_count VALUES (123456789012345678901234567890.1),(123456789012345678901234567890.10),(123456789012345678901234567890.2);
SELECT count(DISTINCT v) FROM diff_typed_count;
DROP TABLE diff_typed_count;
