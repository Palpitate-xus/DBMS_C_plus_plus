-- Database-default text comparison must agree with ORDER BY under en_US.utf8.
DROP TABLE IF EXISTS locale_predicate;
CREATE TEMP TABLE locale_predicate (v TEXT);
INSERT INTO locale_predicate VALUES ('a b'), ('NULL');
SELECT v FROM locale_predicate WHERE v < 'NULL' ORDER BY v;
SELECT v FROM locale_predicate WHERE v > 'a b' ORDER BY v;
CREATE INDEX locale_predicate_v_idx ON locale_predicate(v);
SELECT v FROM locale_predicate WHERE v < 'NULL' ORDER BY v;
SELECT v FROM locale_predicate WHERE v > 'a b' ORDER BY v;
