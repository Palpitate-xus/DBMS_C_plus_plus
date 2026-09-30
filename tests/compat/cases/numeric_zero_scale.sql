SELECT '-0.00'::numeric AS a,'0.000'::numeric AS b,-'0.000'::numeric AS c;
SELECT '0.00'::numeric+'1.0'::numeric AS a,'0.00'::numeric*'1.0'::numeric AS b,'1.25'::numeric-'1.25'::numeric AS c;
SELECT round('0.001'::numeric,2);
DROP TABLE IF EXISTS diff_numeric_zero_values;
CREATE TABLE diff_numeric_zero_values(id INT,v NUMERIC);
INSERT INTO diff_numeric_zero_values VALUES (1,-0.00),(2,0.000);
SELECT id,v FROM diff_numeric_zero_values ORDER BY id;
SELECT count(DISTINCT v) FROM diff_numeric_zero_values;
DROP TABLE diff_numeric_zero_values;
