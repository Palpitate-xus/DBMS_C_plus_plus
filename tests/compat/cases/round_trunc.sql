-- round and trunc semantics
DROP TABLE IF EXISTS diff_rt1
CREATE TABLE diff_rt1 (v numeric)
INSERT INTO diff_rt1 VALUES (42.4), (42.5), (-42.5), (42.4382)
SELECT round(v) FROM diff_rt1
SELECT round(v, 2) FROM diff_rt1
SELECT trunc(v) FROM diff_rt1
SELECT trunc(v, 2) FROM diff_rt1
SELECT trunc(1234.5678, -2), round(1234.5678, -2) FROM diff_rt1
