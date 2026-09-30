CREATE TABLE diff_numeric_cast_division(id INT PRIMARY KEY,n NUMERIC);
INSERT INTO diff_numeric_cast_division VALUES(1,0.00),(2,100000000);
SELECT id,n::numeric/7 AS q,id::numeric/7 AS r FROM diff_numeric_cast_division ORDER BY id;
SELECT id::bigint/7 AS q FROM diff_numeric_cast_division ORDER BY id;
DROP TABLE diff_numeric_cast_division;
