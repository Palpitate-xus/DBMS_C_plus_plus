-- JOIN CASE default names follow the ELSE expression, not the WHEN arm.
DROP TABLE IF EXISTS pgdiff_join_case_a;
DROP TABLE IF EXISTS pgdiff_join_case_b;
CREATE TABLE pgdiff_join_case_a (id INT);
CREATE TABLE pgdiff_join_case_b (id INT);
INSERT INTO pgdiff_join_case_a VALUES (7);
INSERT INTO pgdiff_join_case_b VALUES (7);
SELECT CASE WHEN a.id > 0 THEN a.id ELSE b.id END, CASE WHEN a.id > 0 THEN a.id END, CASE WHEN a.id > 0 THEN 'x' ELSE CAST(b.id AS text) END, CASE WHEN a.id > 0 THEN a.id + 1 ELSE b.id + 1 END FROM pgdiff_join_case_a a JOIN pgdiff_join_case_b b ON a.id = b.id;
SELECT CASE a.id WHEN 7 THEN b.id ELSE a.id END FROM pgdiff_join_case_a a JOIN pgdiff_join_case_b b ON a.id = b.id;
