-- Deduplicate values using their output type before LIMIT/OFFSET.
DROP TABLE IF EXISTS diff_typed_distinct;
CREATE TABLE diff_typed_distinct(id INT,v NUMERIC,t TEXT);
INSERT INTO diff_typed_distinct VALUES (1,0.50,'same'),(2,0.500,'same'),(3,2.00,''),(4,NULL,'NULL'),(5,NULL,NULL);
SELECT DISTINCT v FROM diff_typed_distinct ORDER BY v;
SELECT DISTINCT v AS amount FROM diff_typed_distinct ORDER BY amount;
SELECT DISTINCT t FROM diff_typed_distinct ORDER BY t;
SELECT DISTINCT v,t FROM diff_typed_distinct ORDER BY v,t;
SELECT DISTINCT t,v AS amount FROM diff_typed_distinct ORDER BY t,amount;
SELECT DISTINCT ON (v) v,id FROM diff_typed_distinct ORDER BY v,id;
SELECT DISTINCT v FROM diff_typed_distinct ORDER BY v LIMIT 1 OFFSET 1;
DROP TABLE diff_typed_distinct;
CREATE TABLE diff_typed_distinct(v NUMERIC);
INSERT INTO diff_typed_distinct VALUES (123456789012345678901234567890.1),(123456789012345678901234567890.10),(123456789012345678901234567890.2);
SELECT DISTINCT v FROM diff_typed_distinct ORDER BY v;
DROP TABLE diff_typed_distinct;
