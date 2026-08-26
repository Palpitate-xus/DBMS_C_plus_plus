-- date functions: extract fields and date arithmetic
DROP TABLE IF EXISTS diff_d1
CREATE TABLE diff_d1 (d date)
INSERT INTO diff_d1 VALUES ('2024-03-15'), ('2024-01-01'), (NULL)
SELECT d + 1 FROM diff_d1
SELECT d - 1 FROM diff_d1
SELECT d - DATE '2024-01-01' FROM diff_d1
SELECT extract(year FROM d), extract(month FROM d), extract(day FROM d) FROM diff_d1
SELECT extract(dow FROM d), extract(doy FROM d) FROM diff_d1
