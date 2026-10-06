# Retained scalar-projection execution

## Separate runtime defect

After the independent 21000 creation-site repair (`8770ba41`) and static scalar
output-type repair (`0709d889`), the strengthened unsplit matrix exposed a real
TEMP child with an inner projection alias. It failed with 42P01 because the late
legacy `queryExpr` child parser treated its SQL relation name as a physical heap
name. Simple unaliased legacy init-plans used a different session-aware resolver.
Renaming SQL text or inferring schema from rows would not fix the ownership split.

The retained typed source/cursor executor already owns the genuine whole query,
its source occurrences, query/CTE/view scopes, parameters, lazy children, and
declared output. The ordinary CASE entry point now also activates that same
consumer for scalar SQL-child roles and AST unary +/- roles. It still performs
the existing actual-source shape check and does not fabricate another namespace.
Whole binding is retained by `PreparedWithDmlRuntime::runRead()` and its real
carrier/operator graph, rather than discarded before a legacy reparse.

Quantified dispatch remains earlier and retains its streaming/error-metadata
contract. Read CTEs are genuine providers; writing CTE completion remains part of
the owning statement. Parameters/correlation use the existing typed positional
cells and original child sites. This is not a TEMP SQL rewrite, function-name
whitelist, eager scalar maxRows substitute for ANY/ALL, or rowcount type guess.

## Actual V1 evidence

Private base `0709d889`, no new public layout/header, only main.cpp freshly
compiled over the fully audited matching 58-TU parent group. Build 85612 actually
exited 0; repeat/all58 source/header/flags signatures/configuration stamp matched.
Immutable candidate SHA:
`500573eaa6f8d6d4d104e6569634341e2afe939783d96298cf6e3607d25a0e36`.
Flags were official except O2 replaced by O0, not ROOT normal-O2 proof.

The **same unsplit wire matrix**, retaining original SQLSTATE/OID assertions,
passed (31106 and expanded-source 70612 both actually exit 0). Its actual
PostgreSQL **180006** XML/en_US reference also passed. Added controls cover
TEMP/inner and outer aliases, quoted columns, BIGINT, integer-array OID1007,
BOOL, zero/all-NULL output descriptions, an unknown child before a sibling
sequence writer, and genuine VIEW/JOIN/read-CTE sources with correlated children.
The previous 42P01/25-versus-23 failures remain in parent artifacts.

Twelve serial protocol invocations (90787) actually exited 0: the unsplit matrix,
Q84 plus receiver and twelve TEXT/JSON effect checks, ordinary scalar WHERE/ORDER,
scalar sort slots, WITH scalar, typed EXPLAIN, PL binding, INTO demand,
stored-function atomicity, typed array concatenation, and CASE-NULL demand.
All owned servers ended. Deadlines were unchanged; TMPDIR `/dev/shm` scopes
semantic tests only and is not disk/durability evidence.

The original full protocol on immutable cardinality-only parent previously
passed its original scalar 21000 check then failed at joined-view UPDATE line2764
(40930 actually exit 1). It was not a full pass. A later matching integration
must rerun that original test; the joined-view mutation remains a separate task.

## Integration and explicit open boundaries

The new full scalar matrix is now registered only after actual complete success.
ROOT must still freshly build its combined public-header group: later pure
constant planning, FROM-less root planning, and pure unary-signature selection
are separate dependencies with different headers/ownership flags. In particular,
this consumer's unary AST activation is **not** a claim that the full unary type,
interval, geometry, or other operator family already passed: the separate pure
unary binder/evaluator work and its unchanged strong matrix remain required.

Artifacts: `/tmp/dbms-scalar-retained-runtime.2MLSPrOp/` (`import-baseline.log`,
`build-v1.log`, `reference-expanded-source-unsplit-18.log`,
`wire-expanded-source-unsplit-v1.log`, `wire-adjacent-v1-serial.log`).
No source/test expectation is weakened and no unsupported source is declared
implemented solely by rejecting it.
