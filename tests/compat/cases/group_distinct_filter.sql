-- Aggregate-local filtering retains groups and unfiltered sibling counts.
DROP TABLE IF EXISTS diff_group_filter;
CREATE TABLE diff_group_filter(g INT,v NUMERIC,f INT);
INSERT INTO diff_group_filter VALUES (1,0.50,1),(1,0.500,1),(1,2.00,0),(1,NULL,1),(2,4.00,0),(2,8.00,NULL);
SELECT g,count(DISTINCT v) FILTER (WHERE f=1),count(*) FROM diff_group_filter GROUP BY g ORDER BY g;
SELECT g,count(DISTINCT v) FILTER (WHERE f=0),count(*) FROM diff_group_filter GROUP BY g ORDER BY g;
SELECT g,count(DISTINCT v) FILTER (WHERE f=9),count(*) FROM diff_group_filter GROUP BY g ORDER BY g;
SELECT g,count(DISTINCT v) FILTER (WHERE f=1),count(*) FROM diff_group_filter GROUP BY GROUPING SETS ((g),()) ORDER BY g;
DROP TABLE diff_group_filter;
