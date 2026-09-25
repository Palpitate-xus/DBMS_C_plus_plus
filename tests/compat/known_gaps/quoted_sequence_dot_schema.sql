-- Open TYPE/CAT namespace gap: valid PostgreSQL identifier containing a dot
-- is rejected by the sequence storage name validator. Keep this regression
-- separate from passing differential cases until the storage mapping changes.
DROP SCHEMA IF EXISTS "Diff.Seq.Schema" CASCADE;
CREATE SCHEMA "Diff.Seq.Schema";
CREATE SEQUENCE "Diff.Seq.Schema".seq_start START 5;
DROP SEQUENCE "Diff.Seq.Schema".seq_start;
DROP SCHEMA "Diff.Seq.Schema";
