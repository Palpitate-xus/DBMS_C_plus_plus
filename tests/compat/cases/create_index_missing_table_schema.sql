CREATE INDEX ON diff_create_index_missing_schema.t (id);
CREATE INDEX IF NOT EXISTS diff_create_index_missing_idx ON diff_create_index_missing_schema.t (id);
SELECT 1;
