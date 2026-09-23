-- Integer constant comparisons and UNKNOWN are valid ON predicates.
DROP TABLE IF EXISTS pgdiff_join_const_cmp_l;
DROP TABLE IF EXISTS pgdiff_join_const_cmp_r;
CREATE TABLE pgdiff_join_const_cmp_l (id INT);
CREATE TABLE pgdiff_join_const_cmp_r (id INT);
INSERT INTO pgdiff_join_const_cmp_l VALUES (1), (2);
INSERT INTO pgdiff_join_const_cmp_r VALUES (3);
SELECT l.id, r.id FROM pgdiff_join_const_cmp_l l JOIN pgdiff_join_const_cmp_r r ON 1 = 1 ORDER BY l.id;
SELECT l.id, r.id FROM pgdiff_join_const_cmp_l l JOIN pgdiff_join_const_cmp_r r ON 1 = 0;
SELECT l.id, r.id FROM pgdiff_join_const_cmp_l l LEFT JOIN pgdiff_join_const_cmp_r r ON 2 < 3 ORDER BY l.id;
SELECT l.id, r.id FROM pgdiff_join_const_cmp_l l LEFT JOIN pgdiff_join_const_cmp_r r ON 2 > 3 ORDER BY l.id;
SELECT l.id, r.id FROM pgdiff_join_const_cmp_l l RIGHT JOIN pgdiff_join_const_cmp_r r ON NULL;
SELECT l.id, r.id FROM pgdiff_join_const_cmp_l l FULL OUTER JOIN pgdiff_join_const_cmp_r r ON 1 = 0 ORDER BY l.id NULLS LAST, r.id NULLS LAST;
