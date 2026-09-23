-- Blank-padded CHAR comparisons and real SQL NULL in projected expressions.
DROP TABLE IF EXISTS pgdiff_char_comparison;
CREATE TABLE pgdiff_char_comparison (k INT, c CHAR(3), v VARCHAR(3));
INSERT INTO pgdiff_char_comparison VALUES (1, 'a', 'a'), (2, NULL, NULL), (3, '', '');
SELECT k, c = 'a', c = 'a ', c = v, c IS NULL, v IS NULL FROM pgdiff_char_comparison ORDER BY k;
