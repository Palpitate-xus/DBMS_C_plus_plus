-- Casting a collated text value to integer removes its text collation.
SELECT CASE WHEN true THEN ('1' COLLATE "C")::integer ELSE ('2' COLLATE "default")::integer END AS result;
SELECT COALESCE(('1' COLLATE "C")::integer, ('2' COLLATE "default")::integer) AS result;
