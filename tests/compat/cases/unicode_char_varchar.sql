-- Character limits count UTF-8 characters, while CHAR pads shorter values.
DROP TABLE IF EXISTS pgdiff_utf8_width;
CREATE TABLE pgdiff_utf8_width (c CHAR(3), v VARCHAR(3));
INSERT INTO pgdiff_utf8_width VALUES ('汉字é', 'ééé');
SELECT c, v, length(c), length(v) FROM pgdiff_utf8_width;
UPDATE pgdiff_utf8_width SET c = 'éé', v = '汉字é';
SELECT c, v, length(c), length(v) FROM pgdiff_utf8_width;
UPDATE pgdiff_utf8_width SET c = 'éééé';
SELECT c, v FROM pgdiff_utf8_width;
UPDATE pgdiff_utf8_width SET v = 'éééé';
SELECT c, v FROM pgdiff_utf8_width;
