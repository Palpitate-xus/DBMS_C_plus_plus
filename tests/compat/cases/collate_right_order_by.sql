-- right(text, integer) must be evaluated before ORDER BY comparison.
CREATE TEMP TABLE diff_collate_right_order(v text);
INSERT INTO diff_collate_right_order VALUES ('Zoo'), ('apple'), ('Banana');
SELECT v FROM diff_collate_right_order ORDER BY right(v, 2) COLLATE "C";
SELECT v FROM diff_collate_right_order ORDER BY right(v, -1) COLLATE "C";
SELECT v FROM diff_collate_right_order ORDER BY right(v, +2) COLLATE "C";
SELECT v FROM diff_collate_right_order ORDER BY right(v COLLATE "C", 2);
DROP TABLE diff_collate_right_order;
CREATE TEMP TABLE diff_collate_right_unicode(v text);
INSERT INTO diff_collate_right_unicode VALUES ('a€'), ('aÿ');
SELECT v FROM diff_collate_right_unicode ORDER BY right(v, 1) COLLATE "C";
DROP TABLE diff_collate_right_unicode;
