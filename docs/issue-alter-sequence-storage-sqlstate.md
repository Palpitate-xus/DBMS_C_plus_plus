# ALTER SEQUENCE must retain storage failure SQLSTATE

The storage API correctly rejects an invalid ALTER with `DBStatus::INVALID_VALUE`,
but DdlExecutor previously printed only `ALTER SEQUENCE failed`. The wire adapter
therefore reported XX000 instead of the real 22023.

The original statement is retained unchanged:

```sql
CREATE TEMP SEQUENCE descending_default INCREMENT -2 NO MINVALUE NO MAXVALUE;
SELECT nextval('descending_default'); -- -1
SELECT nextval('descending_default'); -- -3
ALTER SEQUENCE descending_default INCREMENT 2 START 1 RESTART;
```

INCREMENT alone does not change the existing descending MAXVALUE -1, so START
1 is invalid. The follow-up nextval must remain -5. An explicit NO MINVALUE/
NO MAXVALUE request then legitimately permits the ascending restart.

The repair captures the actual returned DBStatus and uses the existing shared
`sqlstateForDBStatus` mapping; it does not parse messages or map every failure
to 22023. Snapshot/rollback/catalog ownership are unchanged. There are no
public-header, layout or source-manifest changes.

Evidence under `/tmp/dbms-sequence-rollback.ORD02uPL/`:

- Strict 180006 `alter-sqlstate-reference18.log` passed the original statement,
  unchanged counter, explicit valid restart, invalid bound/zero increment/
  zero cache, 25P02 and ROLLBACK TO recovery controls.
- `candidate-v3/sequence_alter_sqlstate.final.log` failed its native assertion
  (exit 134): the output did not contain SQLSTATE 22023.
- `alter-sqlstate-baseline-v3-tmpfs.log` records actual wire XX000 (exit 1).
- `alter-sqlstate-candidate-v4-tmpfs.log` passes the same complete wire script.
  Both use explicit TMPDIR=/dev/shm, original SQL and the original 15-second
  socket deadline. Earlier disk startup/CREATE timeout logs remain retained;
  they are not relabeled as SQLSTATE failures or performance successes.
- Matching V4 native `sequence_alter_sqlstate.log` passed; the same group also
  passed sequence generation, allocation position, old-file migration and
  the original complete sequence native test with independently adapted
  PostgreSQL-backed fixtures.

V4 SHA256: `b8075cab3afc691c47ef09f587da9ee5b18790e3eb22da5217a96cdd86b78325`.
Its full-58-TU O0 basis and unchanged 100 headers are audited; the only new
object after V3 is DdlExecutor. This is not ROOT's formal O2 combination, nor
closure of all sequence/DDL correctness or the original full DDL timeouts.
