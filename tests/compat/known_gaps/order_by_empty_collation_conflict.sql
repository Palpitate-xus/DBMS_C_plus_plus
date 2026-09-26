CREATE TEMP TABLE diff_empty_collation_conflict(v text);
SELECT v FROM diff_empty_collation_conflict ORDER BY replace(v COLLATE "C", 'a' COLLATE "default", 'z');
DROP TABLE diff_empty_collation_conflict;
