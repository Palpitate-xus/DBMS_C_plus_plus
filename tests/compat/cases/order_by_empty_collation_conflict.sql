CREATE TEMP TABLE diff_empty_collation_conflict(v text);
SELECT v FROM diff_empty_collation_conflict ORDER BY replace(v COLLATE "C", 'a' COLLATE "default", 'z');
SELECT v FROM diff_empty_collation_conflict ORDER BY replace(v COLLATE "C", 'a', 'z') COLLATE "default";
SELECT v FROM diff_empty_collation_conflict ORDER BY concat_ws('-', v COLLATE "C", 'a' COLLATE "default");
DROP TABLE diff_empty_collation_conflict;
