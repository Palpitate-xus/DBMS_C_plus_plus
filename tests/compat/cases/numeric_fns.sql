DROP TABLE IF EXISTS diff_numfn;
CREATE TABLE diff_numfn (v NUMERIC);
INSERT INTO diff_numfn VALUES (2.5), (-2.5);
SELECT abs(v) FROM diff_numfn ORDER BY v;
SELECT round(v) FROM diff_numfn ORDER BY v;
SELECT log(2, 8);
SELECT trunc(2.71828, 2);
