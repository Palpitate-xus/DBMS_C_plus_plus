-- WHERE and OR must retain SQL bag multiplicity across identical join rows.
DROP TABLE IF EXISTS pgdiff_join_bag_l;
DROP TABLE IF EXISTS pgdiff_join_bag_r;
CREATE TABLE pgdiff_join_bag_l (id INT, v TEXT);
CREATE TABLE pgdiff_join_bag_r (id INT);
INSERT INTO pgdiff_join_bag_l VALUES (1, 'x'), (1, 'x'), (2, 'y');
INSERT INTO pgdiff_join_bag_r VALUES (1), (2);
SELECT l.id, l.v FROM pgdiff_join_bag_l l JOIN pgdiff_join_bag_r r ON l.id = r.id WHERE l.id = 1 ORDER BY l.id;
SELECT l.id, l.v FROM pgdiff_join_bag_l l JOIN pgdiff_join_bag_r r ON l.id = r.id WHERE l.id = 1 OR r.id = 1 ORDER BY l.id;
SELECT l.id, l.v FROM pgdiff_join_bag_l l JOIN pgdiff_join_bag_r r ON l.id = r.id WHERE l.id = 1 OR l.v = 'x' ORDER BY l.id;
SELECT l.id, l.v FROM pgdiff_join_bag_l l JOIN pgdiff_join_bag_r r ON l.id = r.id WHERE l.id = 1 OR l.id = 2 ORDER BY l.id;
SELECT l.id, l.v FROM pgdiff_join_bag_l l LEFT JOIN pgdiff_join_bag_r r ON l.id = r.id WHERE l.id = 1 OR r.id = 1 ORDER BY l.id;
