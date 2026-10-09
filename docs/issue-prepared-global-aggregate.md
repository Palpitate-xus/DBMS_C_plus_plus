# Typed global aggregate lowering for actual prepared child graphs

The original `SELECT (SELECT count(*) FROM typed_src) AS c;` no longer
returns 0A000. The entire unchanged original derived-type fixture passes.
This is one scoped repair, not completion of QRY-02/QRY-05 or aggregate
compatibility. Five newly exposed numeric/ROW dependencies remain genuine
failed tests and must be repaired separately; no assertion is weakened.

## Implementation and ownership

The actual prepared source graph feeds a global aggregate operator. Original
call sites acquire distinct execution-owned typed result columns; only
execution copies are lowered. The shared AST, raw SQL bytes and statement
parameter frame are unchanged. A closed/restarted cursor gets fresh input
transitions; correlated children retain the caller's actual bound row.

COUNT, SUM, AVG, MIN/MAX and boolean reductions use declared signatures and
typed runtime cells, including FILTER and DISTINCT. Metadata resolves genuine
PostgreSQL overload results: integer SUM is BIGINT, BIGINT SUM is NUMERIC,
VARCHAR/NAME/internal-char MIN is TEXT, and CIDR MIN is INET. Unsupported
JSON/JSONB/bit/UUID MIN raises 42883 even for empty input, not an invented NULL.

Prepared expression identity recognizes aggregate roles instead of resolving
COUNT as a scalar callback. Constant CASE pruning is determined from actual
prepared execution copies. Volatile aggregate inputs in EXISTS retain their
evaluation, as observed on the real PostgreSQL 18.6 reference. No SQL string
matching, per-row SQL interpolation or runtime routine call is used to infer
aggregate metadata.

GROUP/window lowering and inside-call ORDER remain open. The AST still owns
inside-call ORDER as opaque text; this path refuses that unsupported shape
rather than silently discarding it. Complex grouping/correlation validation
and remaining aggregate types/demand semantics still require follow-up.

## Actual evidence and all retained failed generations

Artifacts: `/tmp/dbms-having-grammar.MPk64yc0`.
Reference explicitly verified as PostgreSQL 18.6, server 180006.

| Run | Actual result |
| --- | --- |
| Initial reference | 37 controls including BEGIN, exit 0. |
| Exact previous frozen baseline | 36 controls, 31 failures, exit 1. |
| First own fresh 57 units/Main | 79593/56598 both exit 0. |
| Initial native group | 66851 exit 1: 8/9 pass, COUNT identity throws 42883. |
| Initial complete matrix | 45203 exit 1: 46/47 native and 40/41 protocol pass; only the new entries fail. |
| Pure original identity reproduction | 50683 exit 0: real binder outputs BIGINT, then real prepared identity throws 42883. |
| Corrected compilation | 87612 exit 123: missing explicit AST include; log retained. Main 83444 exits 0. |
| Final own fresh 57 units/Main | 77692/94448 both exit 0; 57+1 actual entries. |
| Extended reference | 45 then 56 controls including BEGIN, all pass. Actual pg_proc signature and CASE/EXISTS effect probes also pass. |
| Final direct native group | 28229 exit 0: 9/9 tests; the new fixture has 29 graph, exact BIGINT, typed NULL, repeated execution, correlation and error assertions. |
| Final complete matrix | 50186 exit 1: all 47 native and 40/41 protocol pass, including every original adjacent entry and the complete default protocol. |
| Entire original derived fixture | 90388 exit 0; original SQL and assertions unchanged. |
| Original BIT differential | 47434 exit 0: 384 cases, zero differences on 180006. |

Final new protocol entry has 55 controls, five failures:

| Unclosed dependency | Actual versus reference |
| --- | --- |
| `SUM(v)` with NUMERIC(20,2) column | 40 versus 40.00. |
| `SUM(DISTINCT v)` | 30 versus 30.00. |
| `SUM(v) FILTER(WHERE b)` | 20 versus 20.00. |
| `SUM(v*2)` | 80 versus 80.00. |
| `MIN(ROW(1,'a'))` | 42883 for ROW versus typed record `(1,a)` / OID 2249. |

`makeDecimalColumn` currently discards precision/scale and native numeric
writers only validate the unbounded value. Repair must persist/apply actual
NUMERIC modifiers through the shared column machinery, not pad this one SUM
query or change its input/expected output. ROW is incorrectly parsed as an
ordinary function; its constructor grammar/datum contract needs an independent
repair that preserves existing IN-list demand semantics.

Final frozen binary: `dbms_main.global-aggregate-signatures.frozen`.
SHA256: `cc70a1f3288cd69f379184d1ed59c57965a1b13653b3e854db7ba91a8e7096f6`.
Sealed inputs: `99d22ede9c9f06c81fe563987c42d36970181b59b2596713ec7e997221232feb`.
All 58 current own receipts, cache signature, no-recompile repeat, source and
frozen comparison checks pass at gate start and terminal. No foreign objects,
changed deadlines, deleted original queries or full-suite pass substitution.

This private source generation is not integrated into master. Root whole
driver 84918 continues on its preceding dc89 source generation. Original
273 item statuses are unchanged; no push or Actions activation.
