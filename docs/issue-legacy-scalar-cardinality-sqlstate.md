# Legacy scalar projection cardinality SQLSTATE

## Actual defect

The original full-protocol query at `scalar_multi_messages` is:

```sql
SELECT id, (SELECT id FROM sub_inner WHERE enabled = 1) FROM sub_outer
```

The matching `c5e94063` candidate parent returned XX000 with the cardinality
message, not required 21000. The exact original SQL was independently reproduced
twice with the expected 21000 assertion unchanged (both diagnostics exit 1).
An operator-native checked-result test also aborted (exit 134) at its exact
21000 assertion. These observations are retained in
`/tmp/dbms-scalar-cardinality-state.Nzmxs1Mo`.

`ScalarSubqueryProjectOp::open()` detected a second inner tuple, closed the
inner source, saved only a string error, and returned false. Checked execution
therefore had no structured SQLSTATE and correctly classified that unstructured
failure as XX000. The error must be created as DbError(21000) at detection; it
must not be guessed later from message text.

## Narrow fix and proof

The operator now throws the structured 21000 before cleanup. A local
once-only inner cleanup boundary closes on success or error, preserves a primary
cardinality/read failure if close fails, and lets a normal-path close error keep
its own identity. Checked execution still independently closes the outer graph.
No public header or output-type metadata changes are part of this root cause.

The private build used the complete audited immutable `c5e94063` fresh-58 O0
header/source/flags group, imported matching objects, then recompiled only
changed `ExecutionPlan.cpp`. Build session 26499 actually exited 0; repeat and
all 58 source/header/flags signatures and configuration stamp matched.
Candidate SHA:
`170e9a964bdf4e61f547ec29d25ab561aae4fef827775d2981a8d11e734a73ba`.
This is not a normal-O2 or newly full-fresh-58 claim for this one-CPP change.

Eight matching native tests (session 32775) actually exited 0, including the new
operator test (direct and checked 21000, NULL tuple cardinality, 0/1 row SQL NULL,
secondary-close priority, exact 22012/58030, one inner close/no outer open before
failure), the existing storage scalar-cardinality test, both unchanged ordinary
scalar host tests, host/cursor ownership, logical source, quantified execution,
and prepared cursor. The exact original wire query returned 21000/no rows/no
success tag; the following SELECT 1 recovered.

## Strengthened whole wire matrix: independent failure remains

The new **unregistered**
`scalar_projection_cardinality_sqlstate_protocol_e2e_test.py` retains the exact
original query, NULL multirow errors, successful empty/NULL/empty-text/literal
NULL distinctions, positive OID assertions, and autocommit/savepoint recovery.
Its strict actual PostgreSQL 180006 reference passed. Candidate cardinality
controls passed, but its strengthened positive type assertion revealed another
real defect: the integer scalar child advertised OID 25 instead of 23, while
the rows were correct. Baseline and this candidate have the same mismatch.

That expectation remains **23**, and the full matrix remains failing rather
than being registered or relabelled as green. Scalar descriptor preparation is
the immediately following independent repair. This commit does not claim the
whole matrix or full original protocol passed, nor that every scalar-query
shape is complete. The original full protocol remains unchanged; a matching
immutable candidate run is still required (the parent also has the separately
known earlier FROM-less/PL destination-effect failure).

All deadlines and SQLSTATE assertions remain intact. Protocol semantic probes
used task-scoped TMPDIR `/dev/shm`, not disk-performance/durability evidence.
