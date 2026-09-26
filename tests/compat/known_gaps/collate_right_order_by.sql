-- right(text, integer) must be evaluated before ORDER BY comparison.
CREATE TEMP TABLE diff_collate_right_order(v text);
INSERT INTO diff_collate_right_order VALUES ('Zoo'), ('apple');
SELECT v FROM diff_collate_right_order ORDER BY right(v, 2) COLLATE "C";
DROP TABLE diff_collate_right_order;
