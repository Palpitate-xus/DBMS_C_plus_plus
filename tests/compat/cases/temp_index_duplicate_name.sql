CREATE TEMP TABLE diff_temp_index_duplicate_a (id integer);
CREATE TEMP TABLE diff_temp_index_duplicate_b (id integer);
CREATE INDEX diff_temp_index_duplicate_idx ON diff_temp_index_duplicate_a (id);
CREATE INDEX diff_temp_index_duplicate_idx ON diff_temp_index_duplicate_b (id);
CREATE INDEX IF NOT EXISTS diff_temp_index_duplicate_idx ON diff_temp_index_duplicate_b (id);
DROP INDEX diff_temp_index_duplicate_idx;
DROP TABLE diff_temp_index_duplicate_a;
DROP TABLE diff_temp_index_duplicate_b;
SELECT 1;
