-- ORDER BY must compare the evaluated result of a multi-argument text function.
CREATE TEMP TABLE diff_order_replace(v text);
INSERT INTO diff_order_replace VALUES ('aa'), ('ba');
SELECT v FROM diff_order_replace ORDER BY replace(v, 'a', 'z') COLLATE "C";
SELECT v FROM diff_order_replace ORDER BY replace(v COLLATE "C", 'a', 'z');
SELECT v FROM diff_order_replace ORDER BY translate(v, 'a', 'z') COLLATE "C";
SELECT v FROM diff_order_replace ORDER BY concat('q', replace(v, 'a', 'z')) COLLATE "C";
DROP TABLE diff_order_replace;
CREATE TEMP TABLE diff_order_ltrim(v text);
INSERT INTO diff_order_ltrim VALUES ('ba'), ('aa');
SELECT v FROM diff_order_ltrim ORDER BY ltrim(v, 'a') COLLATE "C";
DROP TABLE diff_order_ltrim;
