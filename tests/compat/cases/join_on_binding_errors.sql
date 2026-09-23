-- ON operands must resolve columns before execution and retain SQLSTATEs.
DROP TABLE IF EXISTS pgdiff_join_on_bind_l;
DROP TABLE IF EXISTS pgdiff_join_on_bind_r;
CREATE TABLE pgdiff_join_on_bind_l (id INT, v INT);
CREATE TABLE pgdiff_join_on_bind_r (id INT, v INT);
INSERT INTO pgdiff_join_on_bind_l VALUES (1, 10);
INSERT INTO pgdiff_join_on_bind_r VALUES (1, 20);
SELECT l.id FROM pgdiff_join_on_bind_l l JOIN pgdiff_join_on_bind_r r ON id = r.id;
SELECT l.id FROM pgdiff_join_on_bind_l l JOIN pgdiff_join_on_bind_r r ON z.id = r.id;
SELECT l.id FROM pgdiff_join_on_bind_l l JOIN pgdiff_join_on_bind_r r ON l.missing = r.id;
SELECT l.id FROM pgdiff_join_on_bind_l l JOIN pgdiff_join_on_bind_r r ON missing = r.id;
SELECT l.id FROM pgdiff_join_on_bind_l l JOIN pgdiff_join_on_bind_r r ON l.id = r.id AND missing = 0;
SELECT l.id FROM pgdiff_join_on_bind_l l JOIN pgdiff_join_on_bind_r r ON l.id = r.id;
