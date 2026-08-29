-- WHERE predicates over strict scalar functions: PG three-valued logic
-- treats a NULL argument as Unknown, so length(NULL) never satisfies = or >.
DROP TABLE IF EXISTS diff_sf;
CREATE TABLE diff_sf (k INT, v TEXT);
INSERT INTO diff_sf VALUES (1, NULL);
INSERT INTO diff_sf VALUES (2, '');
INSERT INTO diff_sf VALUES (3, 'abc');
SELECT k FROM diff_sf WHERE length(v) = 0;
SELECT k FROM diff_sf WHERE length(v) > 1;
SELECT k FROM diff_sf WHERE upper(v) = 'ABC';
SELECT k FROM diff_sf WHERE length(v) = 0 ORDER BY k;
