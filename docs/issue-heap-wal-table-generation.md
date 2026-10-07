# Heap WAL must identify a table generation, not its mutable name

The unchanged `foreign_key_action_dml_test` passed its mutations and table
rename, then failed at startup: committed heap image LSN 8248 still named
`parent`, while the current schema and heap were `renamed_parent`. The decoded
CRC-valid record was an AFTER image for transaction 23; its COMMIT was at LSN
28928. Refusing to invent a missing heap protected data, but prevented valid
recovery. Recreating an old table name was worse: historical images populated
the new, initially empty table with its predecessor's row 99.

New tables receive a durable, nonzero, monotonically allocated table-generation
ID independent of their SQL/storage name. The cluster counter is checksummed,
atomically persisted under a cross-process file lock, and outside database
undo snapshots. A backed-up per-database floor and current schema IDs prevent
normal rollback/restored-floor reuse. Caller-provided IDs are never reused
by CREATE. A rename and schema copies preserve the table generation.

Schema format `0x4442000A` explicitly requires a `RID1` trailer. The existing
`0x44420009` decoder remains supported unchanged; no legacy file is silently
migrated. A partial new trailer cannot fall back to a valid legacy schema.
Serialization remains field-by-field, not raw `sizeof(TableSchema)`.

Heap full-page images and TRUNCATE records gain a strict, versioned generation
extension. Durable CREATE/RETIRE lifecycle records carry the actual owning
transaction ID. Recovery builds an exact generation-to-current-schema map
and resolves renames through it. Absence is not proof of deletion: it may
skip an older absent generation only with a later committed retirement, or
a proven uncommitted birth belonging to that image's transaction. Duplicate
current IDs, malformed extensions, and missing live heap files fail closed.
Legacy name-only records cannot be redirected to a newer same-name table.
UNLOGGED heap contents still produce no lifecycle/page WAL in the original
zero-WAL control.

## Retained evidence and boundaries

- Original unchanged failure: `/tmp/dbms-heap-shared-ownership.x89vCqkb/btree-optimized-v4/foreign_key_action_dml_test.log`, exit 134. Three decoded record/transaction inspector logs under the same private artifact tree retain the initial harness omission and corrected actual evidence.
- Genuine original generation failures: `/tmp/dbms-heap-wal-identity.dcGDltHu/baseline-{rename,reuse,drop}.log`, exit 134. Reuse/drop each printed empty before restart and one resurrected row after restart. Baseline rollback V3 is a valid positive; V1/V2 omitted required manual native snapshot flags and are retained as harness failures, not database bugs.
- New-layout V1 all 58 translation units plus fresh stubs: `build-v1.log`, actual exit 0. Sources and all headers have before/final hash audits. V2 changes only the matching TableManage consumer/private inline codec, with immutable other 57 source/objects and unchanged public layout; `build-v2.log`, exit 0.
- V2 native group: `native-v2.log` retains 14 passing controls and the one separately diagnosed same-parent cross-process transaction-counter failure. Passing controls include unchanged foreign-key action recovery, five rename/reuse/drop/TRUNCATE/rollback shapes, seven identity/legacy/missing-file/nonreuse guards, codec bounds, shared heap/B+Tree rows/crash/generation/TOAST, physical backup/restore, UNLOGGED zero-WAL startup, checkpoints and six SSI shapes.
- The unchanged multi-scenario crash row assertions are mandatory. A warmed parent process used a stale TxnIdGenerator horizon after an external writer; standalone fresh-process DROP checks passed, but they do not replace that stronger original loop. The independent transaction-counter fix and combined full loop proof are required before declaring this scoped implementation ready.

All artifacts are under `/tmp/dbms-heap-wal-identity.dcGDltHu/`; this document
does not label intermediate groups as wholly passing. These native storage
tests are not PostgreSQL SQL differential tests. The separate RR/TOAST
fixture correction has strict PostgreSQL 18.6 evidence and its own commit.

This item closes table-birth/name identity only after the combined strong
gate. It does not prove general heap-layout rewrite epochs, physical DROP
atomicity under power loss, arbitrary legacy renamed history reconstruction,
or a complete PITR/SSI/cross-process cache family. Unprovable legacy replay
remains fail-closed, never filename guessing or silently creating a heap.
