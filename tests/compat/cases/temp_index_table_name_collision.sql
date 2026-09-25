CREATE TEMP TABLE diff_temp_index_table_name_a (id integer);
CREATE TEMP TABLE diff_temp_index_table_name_b (id integer);
CREATE INDEX diff_temp_index_table_name_b ON diff_temp_index_table_name_a (id);
CREATE INDEX IF NOT EXISTS diff_temp_index_table_name_b ON diff_temp_index_table_name_a (id);
DROP TABLE diff_temp_index_table_name_a;
DROP TABLE diff_temp_index_table_name_b;
SELECT 1;
