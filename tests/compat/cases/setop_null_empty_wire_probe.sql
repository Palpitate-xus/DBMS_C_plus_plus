-- Set operations must retain distinct SQL NULL, empty text and literal NULL.
SELECT NULL::text AS v UNION ALL SELECT ''::text AS v ORDER BY v NULLS FIRST;
SELECT 'NULL'::text AS v UNION ALL SELECT ''::text AS v ORDER BY v;
SELECT NULL::text AS v INTERSECT SELECT NULL::text AS v;
SELECT ''::text AS v EXCEPT SELECT NULL::text AS v;
CREATE TABLE diff_setop_wire(v text);
INSERT INTO diff_setop_wire VALUES (NULL), (''), ('NULL'), ('a b');
SELECT v FROM diff_setop_wire UNION SELECT v FROM diff_setop_wire ORDER BY v NULLS FIRST;
DROP TABLE diff_setop_wire;
