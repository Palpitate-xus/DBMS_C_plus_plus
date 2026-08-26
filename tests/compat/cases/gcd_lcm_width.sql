-- gcd, lcm, width_bucket
DROP TABLE IF EXISTS diff_gl1
CREATE TABLE diff_gl1 (a int, b int)
INSERT INTO diff_gl1 VALUES (12,8),(-12,8),(4,6),(-4,6)
SELECT gcd(a,b), lcm(a,b) FROM diff_gl1
SELECT width_bucket(5.5, 0, 10, 4), width_bucket(-1, 0, 10, 4), width_bucket(3, 1, 9, 3) FROM diff_gl1
SELECT gcd(0, 5), lcm(0, 5) FROM diff_gl1
