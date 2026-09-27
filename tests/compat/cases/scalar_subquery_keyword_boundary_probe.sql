-- SELECT need not be followed by a literal ASCII space inside a subquery.
SELECT (SELECT(1)) AS v;
SELECT (SELECT	2) AS v;
