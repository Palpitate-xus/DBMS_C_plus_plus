-- numeric/trig functions via column (engine) path
DROP TABLE IF EXISTS numt
CREATE TABLE numt (id INT, x NUMERIC, y FLOAT8)
INSERT INTO numt VALUES (1, 0.5, 2.5), (2, 42.4382, 9.0)
SELECT round(x) FROM numt WHERE id = 1
SELECT round(y) FROM numt WHERE id = 1
SELECT ceil(x - 2.8) FROM numt WHERE id = 2
SELECT floor(y) FROM numt WHERE id = 2
SELECT trunc(x, 2) FROM numt WHERE id = 2
SELECT power(x, 2) FROM numt WHERE id = 2
SELECT sqrt(y) FROM numt WHERE id = 2
SELECT exp(x) FROM numt WHERE id = 1
SELECT sin(y) FROM numt WHERE id = 1
