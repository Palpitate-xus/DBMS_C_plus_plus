-- Row-count expressions cannot reference variables of the current query level.
SELECT x FROM (VALUES (1), (2), (3)) AS t(x) ORDER BY x LIMIT x;
SELECT x FROM (VALUES (1), (2), (3)) AS t(x) ORDER BY x OFFSET x;
SELECT y FROM (VALUES (1, 10), (2, 20)) AS t(x, y) ORDER BY y LIMIT x;
