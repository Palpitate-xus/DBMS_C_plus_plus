# Array ALTER unique-index collision SQLSTATE

Rounding NUMERIC array elements during ALTER COLUMN TYPE can make two distinct
indexed arrays equal. PostgreSQL 18.6 rejects rebuilding that unique index
with 23505 and restores the original table. The existing DDL executor reduced
the storage primitive's DUPLICATE_KEY result to a generic `Column failed`
diagnostic, which the protocol reported as XX000.

The ALTER TYPE branch now raises the existing structured DbError with 23505
for this storage result. Its existing statement snapshot rollback restores
the original heap, schema, and indexes. No recovery-generation or WAL-format
change was needed for this SQLSTATE correction.

The original full expanded array fixture remains intact: the preserved V4
whole-fixture failure is `/tmp/dbms-alter-array-values.yC5kFpDd/wire-v4-full.log`.
The V5 whole fixture passes, including both unchanged rows `{1.21}` / `{1.24}`
and the index lookup after the rejected NUMERIC(3,1)[] conversion. The complete
strict PostgreSQL 18.6 reference passes the same matrix. Four adjacent,
unchanged array/temporary ALTER protocol fixtures also pass in one serial
wrapper. All 58 normal O2 production source/header/flags/object receipts,
repeated build, and binary stamp match. Default protocol timeouts remain
unchanged; tmpfs test data establishes semantics, not disk performance.
