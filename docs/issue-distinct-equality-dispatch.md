# DISTINCT comparison dispatch

IS DISTINCT FROM now negates the actual equality operator for two nonNULL values, rather than calling an inequality operator. IS NOT DISTINCT FROM preserves the inverse result, and the existing two NULL rules remain unchanged. This matters for PATH and LINE, whose equality is not display-string identity, and for CIRCLE NaN, whose builtin inequality is not the inverse of equality.

The execution rule follows the official PostgreSQL 18.6 [EEOP DISTINCT implementation](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/executor/execExprInterp.c). General NULL behavior is described in the [comparison predicate documentation](https://www.postgresql.org/docs/18/functions-comparison.html). The actual strict PostgreSQL 18.6 controls, including the geometric NaN exception, determine the regression expectations.

## Actual failure

The native baseline returns 2 for `CIRCLE '<(0,0),NaN>' IS DISTINCT FROM CIRCLE '<(0,0),NaN>'`, where PostgreSQL returns 1. The protocol baseline retains 19 incorrect-result assertions after the independent geometric equality value fix. Thus this is a separate dispatch defect, not a missing value codec or a changed test expectation.

The eighteen original geometric IS NOT DISTINCT FROM queries from the unsplit matrix are all retained verbatim in the new permanent test. Additional controls retain both predicate polarities, Boolean OID 16, VALUES, zero rows, all NULL combinations, physical source NULLs, and RHS routines. Runtime source NULL still evaluates the RHS; even a literal NULL does not suppress a nonstrict DISTINCT RHS call. WHERE false demands no call.

## Verification

Artifacts are under `/tmp/dbms-distinct-equality.U7LQ6fUZ`.

| Proof | Result |
| --- | --- |
| `native.baseline.log` | Actual Circle NaN assertion failure retained |
| `baseline.log` | 19 assertion failures retained |
| `reference18.final.log` | Strict PostgreSQL 18.6 version 180006, full strengthened matrix passes |
| `candidate.log` | Full strengthened candidate matrix passes |
| `build.log` | Eight fresh optimized native controls pass |
| `asan.log` | DISTINCT, geometric equality, and typed-literal native controls pass |
| `combined.protocol.log` | Original unsplit matrix and strengthened equality, literal, ordinary CASE source matrices all pass |

The original unsplit matrix is replayed from `original-full-geometry-protocol.py`, which changes only its repository path for the test harness, not any SQL or assertion. The reference fixture uses a unique schema within BEGIN and final ROLLBACK. The candidate evaluator and stubs are fresh; every header, other 57 production source files, and flags match the immutable private 58-object donor. Its SHA256 is `f67bcc939949c98b4b44ec795622a7f961527a9a97a9adeb8b922868ea6689ff`. Sanitizer coverage is scoped to five translation units with other matching objects uninstrumented and leak detection disabled, not a fully sanitized production or full repository gate.

## Scope

This corrects the runtime DISTINCT operator dispatch. It does not add a complete static comparison operator catalog, implicit coercion rules for every predicate, geometric ordering, index codecs, custom operator support, or quantified geometry hashing. Earlier grammar and equality root causes remain separate commits. A later production integration with different public headers requires a fresh matching build rather than reusing this private object's older API layout.
