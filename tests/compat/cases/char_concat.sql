-- The text concatenation operator casts bpchar to text, unlike concat().
DROP TABLE IF EXISTS pgdiff_char_concat;
CREATE TABLE pgdiff_char_concat (c CHAR(3), v VARCHAR(3));
INSERT INTO pgdiff_char_concat VALUES ('a', 'a');
SELECT c || 'b', v || 'b', concat(c, 'b') FROM pgdiff_char_concat;
