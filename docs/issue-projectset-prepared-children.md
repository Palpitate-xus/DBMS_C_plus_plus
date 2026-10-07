# ProjectSet retains typed SQL children and root constant planning

The genuine prepared ProjectSet operator already evaluated an array datum and
returned typed element cells, but its factory rejected every scalar/quantified
SQL child with `0A000`. It also did not consume the root constant-planning flag:
reached constant interval overflow/division could be hidden by LIMIT 0.

The factory now uses the same execution-owned carrier and paired child cursor
provider as the other prepared operators. Whole-query metadata binding remains
earlier than child construction, and actual correlations must have a provider
with the existing restart contract. A child does not become unsupported merely
because it exists. Root planning is an explicit, default-false factory option;
main forwards its existing actual-root flag, whereas child/VIEW/CTE construction
retains the false default. Planned array expressions are consumed by the actual
operator rather than discarded as a preflight result.

The only public signature change appends `bool planRootConstants=false` to
`QueryPlanner::buildPreparedSetReturningPlan()`. There is no new production TU.
Its existing seven-argument callers remain source-compatible, but object ABI
changes and a complete matching build is required.

The retained strong matrix has 22 direct queries and 16 TEXT/JSON plain/ANALYZE
EXPLAIN queries. Strict PostgreSQL 18.6 (180006) passes the full matrix. Its old
candidate log retains `0A000`, wrong constant error demand, and the unchanged
ordinary no-FROM UNNEST receiver failures.

Independent candidate evidence under
`/tmp/dbms-explain-root-projectset.ahA83RRE/`:

- `projectset-v1-build.log`: all 58 production sources independently compiled
  at O0 with the new public header, fresh stubs, matching source/header/flags
  audits. The initial native failure was a fixture expecting an empty string
  payload for SQL NULL. The actual API returned `"NULL"` plus a true NULL
  bitmap; the corrected fixture preserves all SQL, bitmap and non-NULL text
  assertions.
- `projectset-v1.wire.log`: whole matrix terminal 1; eight failed assertions
  comprise the independently repaired pruned-cursor demand defect and two
  ordinary bare-UNNEST receivers.
- `projectset-v2.wire.log`: whole matrix terminal 1 after independent PCE
  commit `15e333e4`; only the two ordinary receiver cases remain (six failed
  state/row/descriptor assertions). All other original controls pass, including
  typed NULL/empty/text cells, actual cursor demand, cardinality `21000`, root
  `22008`/`22012` priority, cumulative sequence effects, and writing CTEs.
- Matching native ProjectSet, quantified execution, constant planning and
  prepared-cursor tests all pass, plus the separate PCE demand regression.

This is a pipeline fix, not a claim that the complete matrix or ROOT formal
combination is green. Ordinary bare-SRF dispatch is a separate next commit.
Multi-SRF/physical-source ProjectSet, SRF ORDER/DISTINCT/group lowering and
correlated CTE provider restart remain outside the implemented operator shape.
