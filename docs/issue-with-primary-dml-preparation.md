# WITH primary DML: structured preparation and logical sources

This is a dependency foundation, not a claim that ordinary WITH-final-DML
execution, every assignment type, or the complete WITH/CTE family is fixed.
The following runtime commit must consume the retained AST and logical rows.

## Retained structures

The parser represents a WITH envelope with a genuine INSERT/UPDATE/DELETE
(or parsed MERGE) child in `WithStmt`. SELECT keeps its existing CTE list.
CTE bodies and the primary DML retain original byte spans; executable children
are never discovered by searching text for SELECT/INSERT or removing a prefix.

`PreparedQuery` retains each statement's output descriptor and source
occurrences. A source distinguishes its visible SQL alias, its actual physical
schema/name, and an optional CTE statement identity. Physical DML targets do
not resolve through the logical CTE namespace. A referenced write CTE without
RETURNING raises `0A000`; no fictitious count column is created.

The optional metadata-only `assignmentInput(target, expression, sourceType)`
callback is supplied by the actual engine. This version uses the shared pure
scalar INTERVAL validator. It is not a general assignment coercion engine.
Each VALUES row transforms all sibling expressions before width/contextual
input checks, then proceeds to the next row. Typed INTERVAL literal and
unknown-to-INTERVAL CAST input transformation occurs at that expression's
binding boundary; numeric narrowing/arithmetic/functions are not evaluated.
Derived/CTE unknown outputs become TEXT; INSERT SELECT keeps each direct
non-star expression's actual AST leaf alongside expanded output ordinals.

The prepared SELECT factory has an additional overload accepting a shared
whole statement, its actual SELECT child, a source descriptor/operator,
ancestor RowContext, and child executor. Its existing overload is preserved.
Projection stars use execution-owned positional columns, leaving the retained
AST unchanged. `PreparedSourceRowsOp` obtains typed ordinal cells only on
`next()`: duplicate labels, empty strings, text NULL, SQL NULL, and LIMIT 0 do
not require temporary relations or an internal DDL/command-counter change.
Separate scalar SELECT sites remain separate carrier memo identities.

## Evidence and current boundary

The isolated private build is in `/tmp/dbms-with-final-dml.C9f0t0DH/foundation`.
Session 19217 rebuilt all 58 production objects and fresh test stubs with O0;
the source/header audits passed. The 11-native run 18975 passed. The final
foundation also includes narrow binder/carrier recompilation and expanded
controls: sessions 58155/43056 finished successfully; session 38186 passed all
11 natives in fresh per-test working directories and both final audits.
The final binary SHA256 is
`036030dd7a3d96c29c26a0f37e4b16a60500d7b741c14946fe6c2efb6e4b23a7`.
This is a fresh-header O0 development proof, not a formal O2 ROOT combination.

A repeated interval native initially failed during CREATE DATABASE because
that pre-existing test leaves its database behind. Its failure log is retained
as `interval_storage_range.repeat-residue.log`. Final native invocations use
separate newly-created working directories; no assertion was weakened.

The new mixed-star assignment probe was actually red before the leaf mapping
fix: session 93480 exited 134 because `SELECT *, '2147483648 months'` lost the
trailing direct literal's input context. The original log and binary remain
under `foundation/tests/with_primary_star_red*`.

The PostgreSQL 17.2 diagnostic in `reference-with-target.py/.log` ran five
controls inside BEGIN/savepoints/final ROLLBACK: a same-named CTE does not
shadow an INSERT/UPDATE/DELETE target; sibling base-table writes are invisible
to the statement snapshot; CTE RETURNING rows can communicate the new values.
This is not a PostgreSQL 18.6 reference run. The target semantics also follow
the [PostgreSQL 18 WITH documentation](https://www.postgresql.org/docs/18/queries-with.html#QUERIES-WITH-MODIFYING).

The separately preserved original wire fixture is still a runtime red gate:
WITH-final INSERT returned 42703 instead of 22015, and a writing CTE consumed
a sequence value before the later input error. It is not removed or labeled
green by this foundation.

Common-type conversion of all-unknown/mixed CASE arms, general operator and
assignment typing, recursive/join/group/window logical source lowering,
MERGE execution, and every ordinary WITH runtime shape remain separate work.
No complete-family or complete-project status follows from these checks.
