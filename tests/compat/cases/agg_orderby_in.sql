DROP TABLE IF EXISTS diff_ordagg137;
CREATE TABLE diff_ordagg137 (g VARCHAR(4), v INT);
INSERT INTO diff_ordagg137 VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 15), ('b', 15);
SELECT array_agg(g ORDER BY v) FROM diff_ordagg137;
SELECT array_agg(g ORDER BY v DESC) FROM diff_ordagg137;
SELECT string_agg(g, '-' ORDER BY v) FROM diff_ordagg137;
SELECT string_agg(g, '-' ORDER BY v DESC) FROM diff_ordagg137;
SELECT sum(v ORDER BY v) FROM diff_ordagg137;
