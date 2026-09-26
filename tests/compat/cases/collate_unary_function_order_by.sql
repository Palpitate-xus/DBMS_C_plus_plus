-- A collation inside a unary text function must control the ORDER BY key.
CREATE TEMP TABLE diff_collate_unary_order(v text);
INSERT INTO diff_collate_unary_order VALUES ('éclair'), ('zoo');
SELECT v FROM diff_collate_unary_order ORDER BY lower(v COLLATE "C");
SELECT v FROM diff_collate_unary_order ORDER BY upper(v COLLATE "C");
SELECT v FROM diff_collate_unary_order ORDER BY lower(v) COLLATE "C";
SELECT v FROM diff_collate_unary_order ORDER BY upper(v) COLLATE "C";
DROP TABLE diff_collate_unary_order;
