# Runtime parameter child context and cache ownership

## Independent actual root cause

The explicit-origin and typed-child repairs `b1a60a90` / `ccb8210e` do not
repair every parameter consumer. An unchanged direct API probe uses real
`QueryBindingDatum`, a genuine prepared child and a real INSERT-writing routine.
It changes actual runtime/metadata caller cells from NULL to `00` / `01`.
The origin-only baseline fails six of twelve controls: its scalar initplan
memo retains the first NULL and one writer effect instead of three. True
statement-input memo is correct. The typed-child carrier alone is insufficient.

A separate unchanged 24-control probe uses the real QueryPlanner child cursor
for ANY and ALL, with NULL / 0 / 1 inputs. The typed-carrier baseline fails
twelve controls: its quantified spool/hash likewise retains the first NULL.
The provider also overwrites caller runtime cells with the frozen query frame.
No fake result reader, datum-value role guess or routine execution to discover
types is used to establish these failures.

## Finite implementation

The original AST owns each parameter's explicit `ParameterOrigin`. Structural
statement ancestry classifies whether a retained child contains a runtime or
metadata parameter. Its slot, source spelling, value and NULL bit do not infer
origin. Scalar memo is allowed only for a child without external column
correlations and without such parameters. Quantified dependent children require
the actual provider's restart contract, clear invocation-specific spool/hash
state and rebind their original operator graph. Genuine statement-only children
retain their original memo/cache behavior.

`PreparedQueryExecution::context(caller)` preserves the caller's real bound
columns and legacy values, but starts with the query's frozen parameter frame.
Only original runtime/metadata nodes admit typed caller cells. Their actual
frame width and declared type are checked without evaluating an expression.
An absent caller frame retains the owned input datum as its execution default;
it does not turn that datum into a constant. Foreign statement-only caller
frames are ignored, not adopted or rejected by a runtime-width heuristic.

Five actual planner consumers now use that channel: physical and logical SELECT
row contexts, one-time filter contexts, set-returning contexts and VALUES cells.
The new RowContext accessor exposes frame width, not role. No parameter value,
source range, original AST coordinate, public enum/window/CTE/signed-FETCH field,
catalog identity or QueryBindingDatum default is removed.

The real VALUES source gains an optional rebinder, retaining the old two-argument
constructor contract. Its independent CPP-local execution wrapper owns begin /
close lifetime. A new root cursor invocation creates a fresh execution carrier,
but the actual source/operator graph remains the same. Explicit close shuts
down the carrier's child cursors; metadata-only never-opened close does not.
Borrowed UNION providers remain owned by Append and do not close sibling graphs.
The reader always consumes its live provider. Primary SQL errors keep precedence
over cleanup errors. No execution retry follows an opened source or effect.

## Strong permanent and unchanged controls

- `runtime_parameter_child_context_test.cpp`: 52 controls, retaining original12
  scalar cases and adding a second genuine cursor owner, explicit frame/type/
  width guards, true frozen statement NULLs, pure preparation and LIMIT 0 / 1
  values and writer counts.
- `runtime_parameter_quantified_context_test.cpp`: 88 controls, retaining the
  original24 SELECT-based ANY/ALL cases, actual graph identities and pure
  admission; standalone VALUES checks real rebinds and typed NULL / 0 / 1 cells.
  This standalone VALUES API is not the unsupported ANY(VALUES ...) grammar.
- `runtime_parameter_values_scope_test.cpp`: 37 controls, four original SELECT /
  VALUES scalar/ALL SQL shapes, three origins, three root cursor invocations,
  exact typed values, writer effects and same-graph ownership.
- `runtime_parameter_set_branch_context_test.cpp`: 91 controls, preserving the
  complete external cursor/reader/borrowed-provider matrix described below.

The separate unchanged external12/24/36 probes all pass on the final candidate.
The first VALUES scope probe proves four introduced restart differences on the
v2 snapshot: statement-only scalar/ALL children retain one writer effect across
three root cursor invocations. The v3 lifecycle change fixes all four. This is
distinct from correctly memoizing repeated evaluation within one execution.
Strict PG18.6 `180006` repeats the same four SQL shapes through real Parse /
Describe / Bind / Execute twelve times, checking each invocation's one writer
effect, values, NULL flags, OIDs and widths. Owned reference schemas are removed
in `finally`; no shared public schema is changed.

An additional unchanged external91-control matrix exercises the legal
SELECT-header UNION ALL VALUES route, not ANY(VALUES ...). It checks real ANY /
ALL cursor graphs, actual reader callbacks which build original-Stmt native
cursors, three origins and NULL / 0 / 1; plus a never-opened borrowed VALUES
branch followed by two complete restarts with exact writer totals 1 / 3 / 5.
All91 pass. These native owners do not prove Main's separate full-host lowering.

## Private evidence and explicitly retained failures

Base `ccb8210e` over immutable Root104+d14 `86a8e6e6`.
Private worktree `/tmp/dbms-runtime-parameter-context.TTn7u16F/repo`.
The three public-header changes receive a genuine fresh all-58-TU normal build,
without borrowing old-ABI objects. The first build/repeat/all58 receipt stage
in session61232 succeeds and has exactly58 compilation lines; its whole native
wrapper exits1 for the new foreign-frame guard and unsupported VALUES grammar.
Both logs remain retained. Session73158 fixes the guard by one CPP recompile
with unchanged headers, verifies all58 current source/header/flag receipts and
the build stamp, and completes all11 native fixtures with exit0.

The independently demonstrated VALUES execution-lifetime correction changes
only Exec CPP, not the three public-header bytes. Session38167 verifies the
current normal/repeat/all58 receipts and all13 complete native fixtures with
exit0, including new52/88/37, original127/587/25, actual host ownership, snapshot
context, scalar/quantified cursor demand, error priority and real Append restart.
All objects come from this tree's own fresh current-ABI normal build.

Frozen v3 binary:
`/tmp/dbms-runtime-parameter-context.TTn7u16F/dbms_main.runtime-context-v3.frozen`.
SHA256 `636e7a2f370329f272a11b3d2393d9ce9fe089ac4250d13ce16237e2c4bb5c17`.

| Complete control | Actual result | Evidence in this artifact directory |
| --- | --- | --- |
| Unchanged external12/24/36 native | 0, session15071 | `runtime-context-original12-24-36-native-v3.log` |
| Legal set/reader/borrowed-provider91 native | 0, session16696 | `runtime-context-set-branch-v3.log` |
| Strict + candidate112/12/190/20 whole fixtures | 0, session14013, eight whole runs | `runtime-context-full-demand-v3.log` and per-file v3 logs |
| Root78 complete whole fixtures | 0, session91580 | `runtime-context-root78-wire-v3.log` |
| Root94 complete native fixtures | 0, session19467 | `runtime-context-root94-native-v3.log` |
| Complete post13 whole fixtures | 0, session21723 | `runtime-context-post13-v3.log` |
| Original parent32 | 1, only two old COUNT FILTER cases, session41446 | `runtime-context-original32-v3.log` |

After these complete collections terminate, the exact unchanged external91
matrix is also added as a permanent native fixture. Normal/repeat/all58 receipt
and four complete new native fixtures finish with exit0 in session22674.
Excluding only this one added test file reproduces the exact v3 input hash
`e5df5bf7dcabb6cec763d9a9a673f623cbfe15855ac133861ba2238e478f9c65`.
Thus every production/header/flag and original94/78 test input remains byte
identical to those completed gates; the production binary is unchanged.

The two unchanged COUNT(*) FILTER BETWEEN cases remain `0A000`, not silently
omitted or reclassified. All26 introduced aggregate NULL writer omissions stay
repaired. The original family273 is not declared complete.

The original `ANY(VALUES(runtime_writer($1)))` / ALL SQL remains a separate
parser issue. All six origin/operator native admission controls return `42883`
on both the frozen old carrier and this candidate. Strict PG180006 executes
both original SQL shapes with NULL / 0 / 1 through real typed protocol calls,
and all six value/NULL/OID/effect controls pass. Evidence is retained in the
external audit directory, not replaced by a different SQL assertion.

Main's independent complete SQL-host VALUES lowering has not been synchronized
here. Native standalone, cursor and reader successes do not declare every
embedding/full-dispatcher fallback fixed. TYPE-11 type-owner/parser/modifier
gaps and old unsupported aggregate features remain separate. Original whole
INTEGER9649/shared-parameter70 requires the three independent INTEGER commits,
which are not in this snapshot and must be checked in a new composition.

The first v2 post13 collection exits1 for its first fixture's unchanged
`start_ours` ConnectionAbortedError103; no SQL failure or deadline adjustment is
substituted for that result. The complete unchanged v3 collection exits0 in
session21723.
No Root/master write, push, Actions change or filtered security task occurs.
