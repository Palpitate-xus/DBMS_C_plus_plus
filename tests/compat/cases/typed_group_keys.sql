-- NUMERIC group identity ignores display scale, while text identity does not.
DROP TABLE IF EXISTS diff_typed_group;
CREATE TABLE diff_typed_group(v NUMERIC,t TEXT);
INSERT INTO diff_typed_group VALUES (0.50,'0.50'),(0.500,'0.500'),(2.00,''),(NULL,'NULL'),(NULL,NULL);
SELECT v,count(*) FROM diff_typed_group GROUP BY v ORDER BY v;
SELECT t,count(*) FROM diff_typed_group GROUP BY t ORDER BY t;
SELECT v,count(*) FROM diff_typed_group GROUP BY v HAVING count(*)=2 ORDER BY v;
SELECT v,count(*) FROM diff_typed_group GROUP BY GROUPING SETS ((v),()) ORDER BY count(*),v;
DROP TABLE diff_typed_group;
