# Geometric typed literal grammar and input validation

The parser now retains POINT, LINE, LSEG, BOX, PATH, POLYGON, and CIRCLE typed literals as actual literal nodes with their declared type and original source span. Pure preparation validates their input before execution, and evaluation returns the existing geometric codec's canonical text. No query, routine, or source is executed to discover the type.

## Actual failures and correction

Original typed-literal SQL inside CASE returned 42703 or 42601 instead of the expected values and geometric OIDs. The native strict preparation baseline aborted with `unexpected token in SELECT statement: ELSE` and SQLSTATE 42601. The protocol baseline retained 65 failed assertions. An invalid PATH inside a writing CTE also reached a sequence effect before the frontend error; after the correction it returns 22P02 with currval still undefined.

The fix preserves typed literals in SELECT, VALUES, zero-row WHERE, and nested discarded CASE arms. Unknown invalid input remains an analysis-time error even when no row would evaluate that arm. The seven positive OID controls and negative inputs match the strict version-gated PostgreSQL 18.6 reference. CAST comparisons are separately retained in the geometric equality tests and are not substitutes for the original typed-literal SQL.

## Verification

Artifacts are under `/tmp/dbms-geometric-expression.QkNnzDwO`.

| Proof | Result |
| --- | --- |
| `literal.native.baseline.log` | Native 42601 baseline retained |
| `literal.baseline.log` | 65 assertion failures retained |
| `literal.reference18.log` | Strict PostgreSQL 18.6 version 180006, complete unchanged matrix passes |
| `literal.candidate.log` | Complete candidate matrix passes |
| `literal.build.log` | Six fresh optimized native controls pass |
| `literal.adjacent.geometric.log` | Original geometric storage and wire protocol controls pass |
| `literal.asan.log` | Typed literal and genuine source context controls pass |

All public header bytes, other 55 production source files, and flags match the immutable private 58-object donor. Parser, binder, evaluator, and stubs are fresh; source and header audits pass. The binary SHA256 is `6002f7f63a7b2d4465773fc56cce5722f5472393e7c3025467b6171675ae8f1b`. Address and undefined behavior instrumentation covers the three changed translation units plus prepared expression execution and execution plan; the other matching objects are not instrumented, and leak detection is disabled.

## Remaining scope

This closes the typed-literal grammar and existing input codec activation, not geometric operator semantics or all geometric inputs. The separate equality baseline still has 54 incorrect-result assertions: existing CAST inputs and the newly recognized original literals expose the same string-comparison defect. Existing packed POINT storage rejects nonfinite coordinates because its index representation lacks such buckets. Geometric ordering, custom type names, operator catalogs, quantifier hashing, and the full CASE family remain separate work.
