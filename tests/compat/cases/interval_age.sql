-- interval arithmetic and age()
DROP TABLE IF EXISTS diff_iv1
CREATE TABLE diff_iv1 (d date)
INSERT INTO diff_iv1 VALUES ('2024-03-15'), ('2024-01-01'), (NULL)
SELECT d + interval '1 day' FROM diff_iv1
SELECT d + interval '1 month' FROM diff_iv1
SELECT d + interval '1 year 2 mons' FROM diff_iv1
SELECT d - interval '2 days' FROM diff_iv1
SELECT age(d, DATE '2020-01-01') FROM diff_iv1
