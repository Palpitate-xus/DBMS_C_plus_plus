-- integer div() quotient function
DROP TABLE IF EXISTS diff_dv1
CREATE TABLE diff_dv1 (a int, b int)
INSERT INTO diff_dv1 VALUES (9,4),(-9,4),(9,-4),(-9,-4)
SELECT div(a,b) FROM diff_dv1
SELECT div(9.0, 4), div(-9.0, 4) FROM diff_dv1
SELECT mod(a,b) FROM diff_dv1
