-- Internal JOIN ON markers must not be accepted as user WHERE columns.
DROP TABLE IF EXISTS pgdiff_join_marker_l;
DROP TABLE IF EXISTS pgdiff_join_marker_r;
CREATE TABLE pgdiff_join_marker_l (id INT);
CREATE TABLE pgdiff_join_marker_r (id INT);
INSERT INTO pgdiff_join_marker_l VALUES (1);
INSERT INTO pgdiff_join_marker_r VALUES (1);
SELECT l.id FROM pgdiff_join_marker_l l JOIN pgdiff_join_marker_r r ON l.id = r.id WHERE __join_on_true__;
SELECT l.id FROM pgdiff_join_marker_l l JOIN pgdiff_join_marker_r r ON l.id = r.id WHERE __join_on_false__;
SELECT l.id FROM pgdiff_join_marker_l l JOIN pgdiff_join_marker_r r ON l.id = r.id WHERE missing;
SELECT l.id FROM pgdiff_join_marker_l l JOIN pgdiff_join_marker_r r ON l.id = r.id WHERE missing = 1;
SELECT l.id FROM pgdiff_join_marker_l l JOIN pgdiff_join_marker_r r ON l.id = r.id WHERE id = 1;
SELECT l.id FROM pgdiff_join_marker_l l JOIN pgdiff_join_marker_r r ON l.id = r.id WHERE z.id = 1;
SELECT l.id FROM pgdiff_join_marker_l l JOIN pgdiff_join_marker_r r ON l.id = r.id WHERE l.missing = 1;
SELECT l.id FROM pgdiff_join_marker_l l JOIN pgdiff_join_marker_r r ON l.id = r.id WHERE l.id = 1;
