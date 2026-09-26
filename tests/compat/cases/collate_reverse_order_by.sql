-- ORDER BY must compare the evaluated reverse(text) result.
CREATE TEMP TABLE diff_collate_reverse_order(v text);
INSERT INTO diff_collate_reverse_order VALUES ('ab'), ('ba');
SELECT v FROM diff_collate_reverse_order ORDER BY reverse(v) COLLATE "C";
DROP TABLE diff_collate_reverse_order;
CREATE TEMP TABLE diff_collate_reverse_unicode(v text);
INSERT INTO diff_collate_reverse_unicode VALUES ('a€'), ('aÿ');
SELECT v FROM diff_collate_reverse_unicode ORDER BY reverse(v) COLLATE "C";
SELECT v FROM diff_collate_reverse_unicode ORDER BY reverse(v COLLATE "C");
DROP TABLE diff_collate_reverse_unicode;
