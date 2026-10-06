# Typed geometric equality values

The evaluator now implements the existing builtin PATH, CIRCLE, LINE, and LSEG equality signatures using geometric values instead of their display strings. PATH equality compares the number of vertices, regardless of coordinates or open versus closed spelling. CIRCLE compares area with PostgreSQL's absolute tolerance. LINE compares proportional coefficients, with the separate exact NaN rule. LSEG compares ordered endpoints, with each point's own NaN rule. NULLIF consumes actual equality rather than generic ordering equivalence.

This follows the official PostgreSQL 18.6 `geo_ops.c` implementations `path_n_eq`, `circle_eq`, `line_eq`, and `lseg_eq`; '=' is not interchangeable with the geometric '~=' operators. Circle inequality is deliberately not the inverse of equality for NaN. Area and coefficient arithmetic retain structured overflow and underflow errors 22003 instead of silently returning a Boolean.

## Actual failures

The native CAST baseline returns 2 for two paths having the same vertex count, where PostgreSQL returns 1. Original typed-literal and CAST protocol queries independently retain that error. The first complete protocol matrix reports 54 incorrect-result assertions. Its IS DISTINCT FROM controls expose a separate operator-dispatch cause and are preserved for the next independent fix.

The strengthened equality matrix has 60 failed assertions against the immutable typed-literal candidate. These include wrong equality values, NaN, proportional lines, tolerance, NULLIF, missing circle range errors, physical nullable columns, and a switch routine that selects the wrong arm. The existing switch-once and runtime NULL source call-count assertions are retained, not relaxed.

## Verification

The original full matrix logs are in `/tmp/dbms-geometric-expression.QkNnzDwO`. Equality artifacts are in `/tmp/dbms-geometric-equality.RrjBHAfm`.

| Proof | Result |
| --- | --- |
| Original `equality.native.baseline.log` | Actual path value assertion failure retained |
| Original `equality.baseline.log` | 54 assertion failures retained |
| `baseline.log` | Strengthened equality matrix, 60 failed assertions retained |
| `reference18.final.log` | Strict PostgreSQL 18.6 version 180006, complete strengthened equality matrix passes |
| `candidate.log` | Complete strengthened candidate equality matrix passes |
| `build.log` | Seven fresh optimized native controls pass |
| `adjacent.protocol.log` | Original typed-literal, geometric wire, and full ordinary CASE source controls pass |
| `asan.log` | Equality and typed-literal native controls pass with scoped instrumentation |

The reference fixture uses a unique schema inside BEGIN, SAVEPOINTs for errors, and final ROLLBACK. A preliminary reference rerun exposed only a duplicate early prototype function; its failure is retained in `reference18.log`, and the exact owned function was verified and removed before the permanent isolated fixture was used.

The changed evaluator and stubs are fresh. All header bytes, other 57 source files, and flags match the immutable private 58-object donor; source and header audits pass. The binary SHA256 is `f7c1e7d80e1b99abd58528d753da2acd86800028d7d8bd0988221b552e19b25e`. Sanitizer instrumentation covers evaluator, parser, binder, prepared expression execution, and execution plan translation units, with other matching objects uninstrumented and leak detection disabled.

## Remaining scope

IS DISTINCT FROM still dispatches inequality rather than negating the selected equality operator and is a separate active fix. The original 18 DISTINCT controls remain part of that independent permanent test. This commit does not implement generic geometric ordering, physical index comparison codecs, custom operators, or quantified geometry hashing. Geometric equality is not transitively hashable merely because the source operator is spelled '='; its fuzzy semantics require actual operator metadata and an appropriate capability before a hash execution path is enabled.
