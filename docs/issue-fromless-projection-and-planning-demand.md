# FROM-less projection and planning demand

WHERE and LIMIT must control a genuine one-row, zero-column SELECT input,
not merely suppress a projection already evaluated by the legacy dispatcher.
The original PL destination script left a sequence at 2 under WHERE false;
actual PostgreSQL 18.6 leaves it at 1. This was a real extra routine call.

The frontend now uses its wholly bound original query and a real typed input
operator for this role. The root opts into constant planning on the same
execution-owned carrier that subsequently prepares and evaluates its targets.
Borrowed child graphs retain their default false root-planning option: they
cannot independently reacquire a discarded CASE or derived/CTE output demand.
The convenience physical-query overload is unchanged; the explicit source
owner is the producer of this planning contract.

## Original failures and strict reference

Artifacts in `/tmp/dbms-pl-destination-demand.mSZvTtPA` retain the original
instrumented eight-case PL baseline/reference and the expanded 24-control
baseline. An initial OFFSET draft was contradicted by real PG18: skipped
FROM-less projections in these queries still evaluate once. Its failed log
is retained; the permanent controls use that verified behavior.

The first candidate passes the original PL script and 21 of 24 new controls,
but silently accepts constant division under WHERE false/LIMIT 0 and a
constant-dividing scalar child. Its six state/tag errors remain in the log.
The unchanged complete 24-control strict reference passes with the actual
server_version_num 180006 gate. No query, assertion, effect sentinel or
deadline was weakened to repair those three constant-planning errors.

## Final candidate evidence

Artifacts: `/tmp/dbms-fromless-root-planning.eCYBCrM3`.

- The first build-and-verify wrapper ends 1 solely because it misspelled the
  existing prepared cursor test filename. Its 12 actual native tests and all
  eight wire entry points pass; the compilation failure remains visible.
- `native-repeat.log`, session27367=0, reruns the entire corrected 13-native
  group with fresh stubs/tests and matching source/header/flag signatures.
- `build-and-verify-v2.log`, session10404=0, freshly compiles all 58 production
  TUs with official flags except O2 replaced by O0, then repeats the build,
  verifies all 58 object signatures/configuration stamp, and passes the
  complete 13-native/eight-wire groups on the refined real root source owner.
- Those eight scripts retain the full24 demand controls, original PL
  destination, whole ordinary CASE/source/VIEW/JOIN, ELSE labels, wide integer
  literals, typed geometry/equality and original84 quantified controls plus
  the ordinary receiver and12 TEXT/JSON plan/demand checks.
- Final source/header/flags/tracked-fixture audit passes. The only difference
  between the copied strict-reference 24-control fixture and this permanent
  file is one trailing blank line; all SQL and assertions are identical.

Final development candidate SHA256:
`b3eda6b5f12e4eea6453339da630d3e655c3c66d1d2f3a8a9d0523a50f407b51`.
Public prepared-plan signature changes require a fresh matching ROOT all-58
normal build and new combination gates. This does not close all logical
output pruning, aggregate/window/SRF demand, immutable-routine folding or the
complete query/planner families. No push or Actions occurred.
