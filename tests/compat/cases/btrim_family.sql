-- btrim, ltrim, rtrim with trim characters
DROP TABLE IF EXISTS diff_bt1
CREATE TABLE diff_bt1 (v text)
INSERT INTO diff_bt1 VALUES ('xxhixx'), ('thighth')
SELECT btrim(v, 'x') FROM diff_bt1
SELECT ltrim(v, 'x'), rtrim(v, 'x') FROM diff_bt1
SELECT btrim(v) FROM diff_bt1
SELECT btrim('xxhith', 'xh') FROM diff_bt1
