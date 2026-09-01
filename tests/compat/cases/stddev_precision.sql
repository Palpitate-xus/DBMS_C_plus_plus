DROP TABLE IF EXISTS diff_stddev135;
CREATE TABLE diff_stddev135 (v INT);
INSERT INTO diff_stddev135 VALUES (10), (20), (5), (15), (15);
SELECT stddev(v), variance(v) FROM diff_stddev135;
SELECT stddev_pop(v), var_pop(v) FROM diff_stddev135;
SELECT stddev_samp(v), var_samp(v) FROM diff_stddev135;
