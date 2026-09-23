SELECT 1 WHERE missing_column = 1;
SELECT 1 WHERE missing_relation.value = 1;
SELECT 1 WHERE coalesce(missing_column, 0) = 0;
SELECT 1 WHERE false AND missing_column = 1;
SELECT 1 WHERE 1 = 1;
SELECT 1 WHERE 1 = 2;
