DROP TABLE IF EXISTS diff_itail;
CREATE TABLE diff_itail (id integer);
CREATE INDEX diff_itail_idx ON diff_itail (id) unexpected_token;
CREATE INDEX diff_itail_idx ON diff_itail (id);
INSERT INTO diff_itail VALUES (1);
SELECT id FROM diff_itail WHERE id = 1;
