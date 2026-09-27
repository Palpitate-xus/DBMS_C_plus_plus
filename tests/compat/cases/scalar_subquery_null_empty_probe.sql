-- Scalar-subquery projection must preserve distinct wire values and types.
SELECT (SELECT NULL::text) AS v;
SELECT (SELECT ''::text) AS v;
SELECT (SELECT 'NULL'::text) AS v;
