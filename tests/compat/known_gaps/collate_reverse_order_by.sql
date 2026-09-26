-- ORDER BY must compare the evaluated reverse(text) result.
CREATE TEMP TABLE diff_collate_reverse_order(v text);
INSERT INTO diff_collate_reverse_order VALUES ('ab'), ('ba');
SELECT v FROM diff_collate_reverse_order ORDER BY reverse(v) COLLATE "C";
DROP TABLE diff_collate_reverse_order;
