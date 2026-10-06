# Sequence allocations must survive DDL snapshot rollback

## Actual regression

The original reproduction creates a TEMP table and TEMP sequence after BEGIN,
allocates 1, creates a savepoint, allocates 2 and 3, and rolls back to the
savepoint. `currval` remains 3 but the next allocation incorrectly returns 2.
Both frozen fd183ec3 (`23a0f0c6...`) and a matching full-58-source O0 build of
4cf76fdd (`b2c36c13...`) reproduced it. PostgreSQL 18.6 / 180006 returns 4.

The savepoint owns an exact database snapshot after physical DDL. Restoring it
restored the sequence's counter file along with transactional metadata, while
the backend's currval cache correctly remained outside transactional undo.
Simply keeping live sequence files is also wrong: RESTART and DROP/recreate
have different storage generations that must not be overlaid on old objects.

PostgreSQL documents that nextval/setval are not undone by rollback, whereas
RESTART is transactional. Its implementation rewrites storage for options
affecting future generation, but not OWNED BY. Sources:
[sequence functions](https://www.postgresql.org/docs/18/functions-sequence.html),
[ALTER SEQUENCE](https://www.postgresql.org/docs/18/sql-altersequence.html), and
`REL_18_STABLE/src/backend/commands/sequence.c`, `init_params`/`AlterSequence`.

## Generation-aware allocation storage

The private `DBMSSEQ5` declaration format retains a storage-generation UUID.
Allocation state belongs to a separately atomically written runtime fork in
the database's `.sequence_runtime` directory. CREATE and options affecting
future generation allocate new generations; rename and OWNED BY retain the
generation. Logical catalog OIDs and backend currval/lastval are unchanged.

Only SQL transaction/statement/savepoint restore overlays the current runtime
forks on its staged restored database. Declaration/ownership/catalog files
come from the snapshot. Retired generation forks are kept until rollback
images are discarded; they let an old stream survive allocations followed by
DROP or RESTART. Commit cleanup removes unreferenced generation forks. Normal
explicit physical restore restores the exact backed-up runtime forks instead.

SEQ2/3/4 and unversioned declarations remain readable. SQL snapshot creation
upgrades old declarations before copying them, so later rollback has an
unambiguous generation. Public headers, class layouts and source manifest are
unchanged; this implementation is private to `TableManage.cpp`.

## Evidence and remaining separate issue

Artifacts are under `/tmp/dbms-sequence-rollback.ORD02uPL/`:

- `sequence-rollback-baseline-4cf.log`: real wire nextval 2 instead of 4.
- `baseline/native-v2.log`: after a verified dirty ALTER image, native nextval
  3 instead of 5, exit 134. The earlier draft fixture assumed ordinary CREATE
  marked the image dirty and failed its setup check; that is not bug evidence.
- `candidate-v1/native-v2.log`: native savepoint/full rollback, lower setval
  with `is_called=false`, transactional RESTART, DROP/recreate, and exact
  explicit physical restore passed.
- `minimal-candidate-v1.log`: unchanged original reproduction returns 4.
- `adj-sequence-v1.log`, `adj-temp-serial-v1.log`: existing protocol guards
  passed unchanged, including backend identity and TEMP SERIAL ownership.
- `protocol-reference18-v3.log`: the permanent expanded protocol script's
  entire SQL/value/state/OID expectations match strict 180006 at port 15486.
- `protocol-metadata-position-baseline-v1.log`: that expanded candidate gate
  still fails on a separate existing ALTER INCREMENT position error: after
  last value 2, switching to descending with MIN 1/MAX 10/CYCLE returns 3,
  while PostgreSQL returns 1 and then 10. This must be fixed independently;
  this generation change alone does not close the complete sequence gate.

Candidate V1 SHA256:
`b342944a0d47ceb83fb3035da1ac193d67cb9e7af033486187546eee3d80bd23`.
The basis is all 58 freshly compiled O0 translation units and matching headers;
the changed TM object and test are then rebuilt. This is not a formal ROOT O2
verification. Neither the original full SERIAL DDL timeout nor general DDL I/O
is claimed fixed by this sequence correctness repair.
