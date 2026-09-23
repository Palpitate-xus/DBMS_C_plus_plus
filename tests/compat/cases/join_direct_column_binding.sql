-- Bare duplicate columns and unknown table aliases must error, not read a side.
DROP TABLE IF EXISTS pgdiff_join_bind_l;
DROP TABLE IF EXISTS pgdiff_join_bind_r;
CREATE TABLE pgdiff_join_bind_l(k INT, v TEXT);
CREATE TABLE pgdiff_join_bind_r(k INT, v TEXT);
INSERT INTO pgdiff_join_bind_l VALUES (1, 'left');
INSERT INTO pgdiff_join_bind_r VALUES (1, 'right');
SELECT v FROM pgdiff_join_bind_l l JOIN pgdiff_join_bind_r r ON l.k = r.k;
SELECT z.v FROM pgdiff_join_bind_l l JOIN pgdiff_join_bind_r r ON l.k = r.k;
SELECT l.missing_v FROM pgdiff_join_bind_l l JOIN pgdiff_join_bind_r r ON l.k = r.k;
SELECT l.v, r.v FROM pgdiff_join_bind_l l JOIN pgdiff_join_bind_r r ON l.k = r.k;
