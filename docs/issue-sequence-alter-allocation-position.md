# ALTER SEQUENCE must use persistent last_value and is_called

## Independent actual regression

The generation rollback repair alone still returned 3 after allocating 1 and
2 and changing INCREMENT to -1 with MINVALUE 1, MAXVALUE 10 and CYCLE. Strict
PostgreSQL 18.6 / 180006 returns 1, then 10. The matching generation-only
candidate failed both `native-position-baseline-v1.log` (exit 134) and
`protocol-metadata-position-baseline-v1.log` (exit 1), preserved under
`/tmp/dbms-sequence-rollback.ORD02uPL/`.

The old implementation recomputed a predecessor around the already computed
next cursor. It also replaced existing implicit bounds merely because the
increment's direction changed. Neither is PostgreSQL's ALTER contract.

## Repair

The private DBMSSEQR2 runtime record stores persistent reservation high-water
`last_value` and `is_called` separately from the next cursor and cache marker.
ALTER invalidates the caller's preallocated cache and derives the new cursor
from that high-water using the new increment. An uncalled sequence, including
`setval(..., false)`, still returns its exact stored value once. RESTART creates
a new uncalled storage generation; SQL rollback restores the old declaration
and keeps allocations in the generation to which they belong.

Existing min/max bounds are retained unless the statement explicitly changes
them. START only changes the recorded restart value. OWNED BY retains its
storage generation but invalidates the backend cache, as PostgreSQL does.
Retired generation forks are reclaimed after SQL owners discard rollback
images, or after native ALTER/DROP when there is no active same-database
transaction. Reclamation uses the global-transaction then sequence-lock order.

Old SEQ2/3/4, unversioned declarations and DBMSSEQR1 remain readable. The old
formats did not retain complete historical `is_called` information: a wrapped
cycle cursor and some old uncalled states can be indistinguishable. Migration
preserves their existing next cursor and persists explicit state on new
allocations; it does not claim to reconstruct history that was never stored.
Public headers, layouts and the 58-source manifest are unchanged.

## Evidence boundary

The basis is a private, fully fresh 58-TU O0 build of 4cf76fdd with 100 matching
headers. Candidate V3 rebuilds its changed TM object and matches that basis:
`df6c86376017a3aed56cacd84bdaf0444d609dbac40072e4a235cb64613d9739`.

- V3 `sequence_ddl_rollback_generation.log` and
  `sequence_alter_position.log` passed real native rollback, RESTART,
  DROP/recreate, cache, uncalled, direction and exhaustion controls.
  `sequence_alter_position.final.log` also verifies direct-native retired-fork
  reclamation leaves exactly four live generation files for four declarations.
- `protocol-reference18-v6.log` passes the complete permanent protocol script
  at strict 180006, including OWNED BY CACHE 5, legacy-equivalent direction,
  OIDs, currval/lastval, savepoint/full rollback and per-row stored writer.
- `protocol-candidate-v2.log` and seven `adj-*-v2.log` passed the previous
  matching metadata candidate and existing sequence/ownership/transaction/
  returning/atomicity guards. `protocol-candidate-v3-final-serial-repeat.log`
  subsequently passes the expanded final script serially. Earlier V3 disk
  startup and CREATE timeouts remain separately recorded; a live worker was
  observed in `jbd2_log_wait_commit`. Do not relabel those failures as green
  or claim this correctness repair fixes that I/O condition.
- The original `candidate-v3/sequence_full.log` is retained with exit 134.
  Its old descending ALTER success expectation was invalid: without explicit
  NO MIN/MAX, START 1 is outside the preserved MAXVALUE -1. Its bounded
  negative-increment and legacy direction expectations also require an
  independent fixture adaptation against real PostgreSQL, not weaker checks.

This is not ROOT's formal O2 combination proof. It does not close all sequence
type-width/legacy-history/crash durability, DDL I/O or full differential gates.
The original full SERIAL/valid CHECK ADD COLUMN timeouts remain unexplained.

Primary contract sources: [ALTER SEQUENCE](https://www.postgresql.org/docs/18/sql-altersequence.html),
[sequence functions](https://www.postgresql.org/docs/18/functions-sequence.html),
and the local official PostgreSQL 18.6 `sequence.c` (`init_params`,
`AlterSequence`, `nextval_internal`).
