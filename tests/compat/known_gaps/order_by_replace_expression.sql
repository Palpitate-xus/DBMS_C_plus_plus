-- ORDER BY must compare the evaluated result of a multi-argument text function.
CREATE TEMP TABLE diff_order_replace(v text);
INSERT INTO diff_order_replace VALUES ('aa'), ('ba');
SELECT v FROM diff_order_replace ORDER BY replace(v, 'a', 'z') COLLATE "C";
DROP TABLE diff_order_replace;
