-- Postfix NULL tests and a nested function must not disappear from FILTER.
DROP TABLE IF EXISTS diff_filter_clause;
CREATE TABLE diff_filter_clause(v NUMERIC,f INT,t TEXT);
INSERT INTO diff_filter_clause VALUES (0.50,1,'other'),(0.500,1,'other'),(2.00,0,'a  )  b'),(NULL,1,'other'),(4.00,0,'other'),(8.00,NULL,'other');
SELECT count(DISTINCT v) FILTER (WHERE f IS NULL),count(DISTINCT v) FILTER (WHERE f IS NOT NULL),count(*) FROM diff_filter_clause;
SELECT count(DISTINCT v) FILTER (WHERE abs(f)=0) FROM diff_filter_clause;
SELECT count(*) FILTER (WHERE f IS NULL),count(*) FILTER (WHERE f IS NOT NULL),count(*) FROM diff_filter_clause;
SELECT count(*) FILTER (WHERE t='a  )  b') FROM diff_filter_clause;
SELECT f,count(DISTINCT v) FILTER (WHERE abs(f)=0),count(*) FILTER (WHERE f IS NULL) FROM diff_filter_clause GROUP BY f ORDER BY f;
DROP TABLE diff_filter_clause;
