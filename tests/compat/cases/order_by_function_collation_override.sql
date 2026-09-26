-- An explicit collation on the function result may override an input collation.
CREATE TEMP TABLE diff_order_collation_override(v text);
INSERT INTO diff_order_collation_override VALUES ('éclair'), ('zoo');
SELECT v FROM diff_order_collation_override ORDER BY replace(v COLLATE "C", 'x', 'x') COLLATE "default";
DROP TABLE diff_order_collation_override;
