-- A top-level COLLATE on ORDER BY left(text, integer) controls the sort.
CREATE TEMP TABLE diff_collate_left_outer(v text);
INSERT INTO diff_collate_left_outer VALUES ('apple'), ('Zoo');
SELECT left(v, 5) AS value FROM diff_collate_left_outer ORDER BY left(v, 5) COLLATE "C";
SELECT left(v, 2) AS value FROM diff_collate_left_outer ORDER BY left(v, 2) COLLATE "C";
SELECT left(v, -1) AS value FROM diff_collate_left_outer ORDER BY left(v, -1) COLLATE "C";
SELECT left(v, +2) AS value FROM diff_collate_left_outer ORDER BY left(v, +2) COLLATE "C";
DROP TABLE diff_collate_left_outer;
