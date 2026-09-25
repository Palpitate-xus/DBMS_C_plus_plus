CREATE TEMP TABLE diff_discard_tag_temp (id integer);
DISCARD SEQUENCES;
DISCARD ALL;
DROP INDEX pg_temp.diff_discard_tag_missing_idx;
SELECT 1;
