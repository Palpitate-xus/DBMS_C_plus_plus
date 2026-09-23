-- Unaliased JOIN function projections use the function's output name.
DROP TABLE IF EXISTS pgdiff_join_head_a;
DROP TABLE IF EXISTS pgdiff_join_head_b;
CREATE TABLE pgdiff_join_head_a (id INT, txt TEXT);
CREATE TABLE pgdiff_join_head_b (id INT, txt TEXT);
INSERT INTO pgdiff_join_head_a VALUES (1, 'alpha');
INSERT INTO pgdiff_join_head_b VALUES (1, 'beta');
SELECT upper(a.txt), coalesce(a.txt, b.txt), abs(a.id) FROM pgdiff_join_head_a a JOIN pgdiff_join_head_b b ON a.id = b.id;
SELECT lower(b.txt) AS result_label FROM pgdiff_join_head_a a JOIN pgdiff_join_head_b b ON a.id = b.id;
