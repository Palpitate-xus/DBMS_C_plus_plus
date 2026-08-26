-- numeric scale rendering in arithmetic
DROP TABLE IF EXISTS diff_ns1
CREATE TABLE diff_ns1 (a int)
INSERT INTO diff_ns1 VALUES (1)
SELECT 7.0/2, 10.0/3, 1.5+1.5, 2.0*3.0, 2.5*2 FROM diff_ns1
SELECT 1.10+2.5, 100.00/4 FROM diff_ns1
SELECT 7/2, -7/2 FROM diff_ns1
