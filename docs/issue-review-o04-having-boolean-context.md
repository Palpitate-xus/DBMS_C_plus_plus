# O04: HAVING requires a BOOLEAN context before execution

## Defect and correction

The grouped legacy receiver accepted non-BOOLEAN HAVING expressions and could
return every group; the global/from-less receivers also mishandled FALSE, NULL
and unknown string input. Empty input could hide the analysis error. HAVING now
uses whole-query binding before source, CTE or volatile target execution.
Non-BOOLEAN typed input raises `42804`; invalid unknown Boolean input raises
`22P02`, including empty-source queries. Unknown strings/NULL receive the genuine
Boolean context, rather than storage-predicate string truth guessing.

Supported prepared global/from-less shapes run through their existing bound
query owner. For the legacy grouped receiver, a verified standalone BOOLEAN
grouping column is lowered to the equivalent `column = TRUE` grammar role;
NULL and FALSE remain excluded. This does not substitute row values into SQL.
Protocol Parse and Describe bind the same query with typed metadata-only
parameters; they do not execute aggregates or sequence functions.

## Evidence

Private worktree: `/tmp/dbms-review-root-having.w4Uj83rC/repo`, based on
`7cae7c1f`. `baseline-protocol.log` preserves the original 41-control failure:
wrong error states, accepted integer/TEXT conditions, incorrect global counts,
and ignored from-less FALSE/NULL. Eight Parse/Describe controls were then added,
not removed from the accepted final driver. Intermediate test-harness mistakes
(missing descriptor helper and wrong descriptor tuple index) remain in their
original logs and are not production failures or passing evidence.

| Verification | Actual result |
| --- | --- |
| PostgreSQL 18.6 final protocol driver, 49 controls | exit 0 |
| Candidate final protocol driver, same 49 controls | exit 0 |
| Native metadata/input fixture, 20 controls | exit 0 |
| Whole native HAVING, query binding, CASE, VALUES and recursive CTE drivers | all five exit 0 |
| Whole HAVING/identifier, grouped identity, global aggregate, aggregate owner, CASE and recursive CTE protocol drivers | all six exit 0 |
| Whole `postgres_protocol_test.py`, first attempt | exit 1; ten-second timeout at existing `ALTER TABLE t ADD COLUMN prepared_note TEXT` |
| Whole `postgres_protocol_test.py`, unchanged driver and timeout, complete retry | exit 0 |

Logs include `reference-accepted-protocol.log`, `candidate-protocol.log`,
`candidate-native.log`, `neighbor-*.log` and
`neighbor-postgres-protocol-retry.log` in the private parent directory. The
original timeout is retained, not replaced by the retry result or hidden by a
longer deadline. Final integrated regression remains a separate gate.

`prove_and_build.sh` verified all 58 donor object receipts, every header,
compiler/flags, source manifest and TLS selection. Only Main, NetworkServer and
query binding differed; those three translation units were genuinely rebuilt,
and the other 55 byte-identical objects received path-only receipts after that
proof. The native gate rechecked every current receipt and unchanged final input
signature. Public PG fixtures use unique names and remove only their own objects.

This correction does not claim all grouped HAVING shapes, all SQL PREPARE
template-analysis phases or complete PostgreSQL query compatibility.
