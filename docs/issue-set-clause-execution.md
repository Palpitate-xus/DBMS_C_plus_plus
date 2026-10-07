# Actual set-root and branch execution

This consumer depends on the independent canonical-cell and set-clause
binding foundations. It lowers retained UNION ALL roots to the real typed
Append graph, then actual common-output Sort/Offset/Limit operators. FETCH
WITH TIES compares real typed key cells and preserves NULL boundaries. It
does not evaluate ORDER expressions a second time, fingerprint row values,
combine distinct SELECT sites, execute rows to infer metadata, render SQL,
or build temporary source tables.

The existing execution root owns one actual compiled carrier and requests
root-only constant planning. Branches/SQL children borrow that carrier and
retain their own genuine local clauses and sites. LIMIT 0 opens no child;
unsorted LIMIT 1 leaves the right branch untouched; a global Sort really
reads its input. Planning still rejects a reached `1/0` before a preceding
writer, including under LIMIT 0. The C++ typed parameter control remains
positional and does not become a reconstructed SQL literal.

The ordinary main entry formerly came *after* `executeSetOperation`, so the
legacy eager route stole plain roots even though the typed API worked.
The entry now precedes that route, also for parenthesized set roots. Whole
metadata binding happens before eligibility/fallback, so unknown calls and
invalid input cannot hide behind a false shape probe. Existing unsupported
aggregate/window/backend/local relational forms keep their existing consumer;
this does not add their execution to the typed Append contract.

## Exact evidence

Artifact directory `/tmp/dbms-set-clause-ownership.oSu5ps6G`:

* `baseline.whole35.log`: matching prior metadata `8c8cf369...`, actual 1,
  original Simple/Describe errors and cumulative extra sequence calls retained.
* Strict PostgreSQL `180006` strengthened whole 43 controls:
  `reference18.whole43.log`, actual 0. It covers exact output types/labels,
  command tags, global/local clauses, NULL/empty/text-NULL, left-associative
  chains, physical/CTE sources, scalar and ANY demand, analysis error
  priorities, root planning, all sequence effects and real SAVEPOINT rollback.
* V1 fresh all58/stubs `53565` actual 0, native3 actual 0;
  `candidate-v1.whole43.log` actual 1 exposed the earlier legacy dispatch,
  not a lowered assertion. New strict typed-NULL native initially failed134
  (`prepared_set_clause_execution.expanded.log`) and was independently fixed.
* Final V2 main/QP/Network fresh on the same matching all58-header epoch:
  `95154`, actual 0, SHA
  `7f0770130838fd605a64b5c505487bc0e498dcc9af94fa3854ba65bc8e330154`.
  Four matching natives all 0 (pure clause binding, original set binding,
  original typed Append plus strict BIGINT NULL, new actual typed clauses).
* The first V2 43/18 launches both finished0 but may have briefly overlapped.
  Their logs remain diagnostic, not final serial proof. The final one-wrapper
  `91315` is authoritative0: **12 complete scripts strictly serial**, including
  all 43 new controls, the unchanged original 18 plus stronger Portal/failed
  named publication controls, original Append/structured sets, both SRF
  matrices, FROM-less demand, Q84, WITH multisource DML, MV target guards,
  EXPLAIN, and PL whole binding. No sequence resets, skipped cases, timeout
  increases, or expected-state reductions were used.
* Four relevant production TUs and three drivers scoped ASan/UBSan `88919`
  actual0; other production objects were not instrumented. Header/source/
  flag/object signatures are matching. Private builds are O0, not normal
  all58 O2 or the canonical full suite.

All registered assertions remain whole and cumulative. This closes these
verified UNION ALL clause shapes, not the whole set/query family: parameter
protocol inference, dynamic clause expressions, locking, unlowered local
relational WITH TIES, broader domains/operator/collation rules, UNION
DISTINCT/INTERSECT/EXCEPT execution and other source forms stay separate.
The independently observed backend-provider explicit search_path issue is
also still open; its PG180006 public-first scalar result is recorded without
altering the previously frozen provider epoch.
