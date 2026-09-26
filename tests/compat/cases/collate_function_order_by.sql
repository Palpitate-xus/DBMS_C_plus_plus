-- ORDER BY respects the explicit collation within a text function argument.
CREATE TEMP TABLE diff_collate_function_order(v text);
INSERT INTO diff_collate_function_order VALUES ('apple'), ('Zoo');
SELECT left(v COLLATE "C", 5) AS value FROM diff_collate_function_order ORDER BY left(v COLLATE "C", 5);
SELECT left(v COLLATE "C", 2) AS value FROM diff_collate_function_order ORDER BY left(v COLLATE "C", 2);
SELECT left(v COLLATE "C", -1) AS value FROM diff_collate_function_order ORDER BY left(v COLLATE "C", -1);
SELECT left(v COLLATE "C", 5) AS value FROM diff_collate_function_order ORDER BY left(v COLLATE "C", 5) COLLATE "C";
SELECT left(v COLLATE "C", 5) AS value FROM diff_collate_function_order ORDER BY left(v COLLATE "C", 5) DESC;
DROP TABLE diff_collate_function_order;
