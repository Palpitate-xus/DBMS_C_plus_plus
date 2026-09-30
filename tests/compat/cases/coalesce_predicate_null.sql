-- A NULL argument is not a strict gate for COALESCE, and text stays text.
DROP TABLE IF EXISTS diff_coalesce_predicate;
CREATE TABLE diff_coalesce_predicate(f INT,t TEXT);
INSERT INTO diff_coalesce_predicate VALUES (0,''),(1,'NULL'),(NULL,NULL);
SELECT count(*) FILTER (WHERE coalesce(f,0)=0) FROM diff_coalesce_predicate;
SELECT f,t FROM diff_coalesce_predicate WHERE coalesce(t,'fallback')='' ORDER BY 1;
SELECT f,t FROM diff_coalesce_predicate WHERE coalesce(t,'fallback')='NULL' ORDER BY 1;
SELECT f,t FROM diff_coalesce_predicate WHERE coalesce(t,'fallback')='fallback' ORDER BY 1;
SELECT count(*) FILTER (WHERE abs(f)=0) FROM diff_coalesce_predicate;
DROP TABLE diff_coalesce_predicate;
