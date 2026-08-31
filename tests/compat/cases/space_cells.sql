DROP TABLE IF EXISTS diff_spacecell;
CREATE TABLE diff_spacecell (filler INT, ts TIMESTAMP, v VARCHAR(20));
INSERT INTO diff_spacecell VALUES (7, '2024-03-15 10:30:00', 'hello world');
SELECT ts FROM diff_spacecell;
SELECT filler, ts FROM diff_spacecell;
SELECT ts, filler FROM diff_spacecell;
SELECT * FROM diff_spacecell;
SELECT v, filler FROM diff_spacecell;
SELECT filler, v FROM diff_spacecell;
