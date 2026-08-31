DROP TABLE IF EXISTS diff_funcs2;
CREATE TABLE diff_funcs2 (v VARCHAR(20), k INT);
INSERT INTO diff_funcs2 VALUES ('Hello World', 5), (NULL, -3), ('x', 0);
SELECT position('World' in v) FROM diff_funcs2;
SELECT sign(k) FROM diff_funcs2;
SELECT btrim(v) FROM diff_funcs2;
SELECT repeat(v, 2) FROM diff_funcs2;
SELECT power(k, 2) FROM diff_funcs2;
SELECT mod(k, 3) FROM diff_funcs2;
