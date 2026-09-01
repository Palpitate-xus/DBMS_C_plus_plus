SELECT array_append(ARRAY[1,2], 3);
SELECT string_to_array('a,b,c', ',');
SELECT cardinality(ARRAY[1,2,3]);
SELECT CASE WHEN 1 > 2 THEN 'yes' END;
SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END;
SELECT 3.7::int;
SELECT 't'::boolean;
