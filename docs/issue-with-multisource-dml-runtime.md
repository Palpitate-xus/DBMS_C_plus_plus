# WITH primary UPDATE FROM / DELETE USING execution

WITH primary multi-source UPDATE and DELETE previously stopped at explicit
`0A000`. The original matrix also exposed a separate comma-source grammar
failure (`42601`), fixed independently in `e1194fa7`. Source occurrence
collisions are handled independently by `c3ff0ac8`, and merged USING type
metadata by `bc1e0bde` (dependency `8e2c2076`). OLD/NEW channels retain the
independent `6b268130` / `ed02ea32` / `d0cb36ca` transition fixes.

## Implementation contract

The original whole PreparedQuery owns every statement, FromItem, bound
column and declaration. Source execution consumes those pointers and real
source/column ordinals; it does not reconstruct SQL, create TEMP surrogate
tables, guess routine datums or use row values as tuple identities.

- Physical sources read through the existing TableScan (including RLS),
  then preserve extracted values, actual NULL bits and types in RowContext
  cells. Logical CTEs use the existing statement-owned returning/materialized
  channels. Retained derived SELECT/VALUES use their actual typed plans.
- CROSS/INNER/LEFT/RIGHT/FULL, USING and NATURAL source trees retain their
  original namespaces. NULL extension supplies typed NULL cells for every
  missing occurrence. USING comparisons and merged outputs apply genuine
  implicit casts to the shared declared type, not a type-label rewrite.
- Qualification and SET projections visit every eligible join tuple. Each
  real target RID is modified once; the chosen eligible tuple supplies both
  its SET values and RETURNING source provenance. SQL leaves the winning
  source unspecified when one target joins multiple source rows, so the
  tests accept either eligible source while requiring consistent values and
  all real projection effects. See [PostgreSQL UPDATE](https://www.postgresql.org/docs/18/sql-update.html).
- Row-independent WHERE false/NULL is checked before opening source rows.
  Metadata preparation never evaluates a routine/child query to obtain a
  value. Runtime qualification does not turn functions/children/source
  columns into this constant shortcut.
- The original storage mutation path still owns locks, RLS, FKs, indexes,
  triggers and physical row rebuilding. RID-aware callbacks are scoped to
  the original target and never inherited by recursive FK mutations.
  Actual OLD/NEW images, source cells and qualified stars stay separate;
  bare RETURNING star follows the real namespace including USING visibility.
- The existing single outer atomic unit/fixed command view is preserved.
  CTE and primary statements do not acquire invented statement CIDs. Late
  failure rolls back their rows, without pretending sequence effects roll
  back; explicit user savepoint recovery retains earlier successful work.

Public additions (all require matching fresh headers/objects):

1. `StorageEngine::SqlMutationCallbacks` supplies matches, resolver and image
   observers with `int64_t rid`. Its optional pointer is appended to the final
   `updateRows` / `removeRows` overloads and internal implementations. Old
   overloads/callbacks remain source-compatible through defaults.
2. `PreparedDmlSourceRows` contains real occurrence ordinals and a typed
   `read(index, RowContext&)`; `PreparedDmlSourceFactory` takes the actual
   owner statement, FromItem and outer row. `prepareBoundDml` and
   `executeBoundDml` append the optional factory. Factory construction is
   metadata-only; only its reader opens/evaluates rows.
3. `PreparedQuery::SourceRange::hiddenUnqualified` retains the binder's actual
   USING/NATURAL and RETURNING transition visibility. Qualified references
   retain their original ordinals; unqualified stars skip hidden columns.

## Retained evidence

Artifacts are under `/tmp/dbms-with-source-runtime.zl5yD3MZ`; these are private
O0 proofs, not a claim that ROOT's eventual combined formal O2 build has run.
The strict reference is real PostgreSQL 18.6 (`180006`) at the isolated 15486
profile. Reference fixtures use temporary objects and preserve all assertions.

| Gate | Actual terminal evidence |
| --- | --- |
| Original expanded 47 controls, strict PG18 | `reference-v2.log`, exit 0 |
| Same 47 controls on frozen ROOT fd183ec3 (`23a0f0c6…`) | 56436 exit 1, `baseline-v2.log`; original SQL/assertions retained |
| First fresh runtime candidate | 36476 all 58 objects/stubs + audits exit 0; native 38039 exit 134 and wire 19215 exit 1 retained (raw TableScan is not a typed cursor) |
| Second candidate after physical-cell adapter/comma parse | 72653 changed parser/main + matching headers exit 0; 77852 two natives exit 0; 58434 main 47 exit 0, boundary exit 1 |
| Extra strict PG18 boundary reference | `boundary-reference-v1.log`, exit 0 |
| Real second-candidate boundary failures | False/NULL WHERE ON sequence calls 2 instead of 55000; FULL USING INT/BIGINT value raises 22003 instead of valid BIGINT output |
| Final fresh new-CastFlag/runtime headers | 40855 all 58 objects/stubs, source/header audits exit 0 |
| Final original 47 + five boundaries | 35473 both gates exit 0 |
| Matching native suite | 95380 ten distinct natives exit 0; 46605 expanded new native + three FK/trigger neighbors exit 0 (13 distinct total, expanded repeat not counted twice) |
| Scoped sanitizer checks | 75199 DML/storage/test ASan+UBSan, three natives exit 0; other objects are matching ordinary O0, not whole-server instrumentation |
| Serial adjacent protocols | 95431 eleven scripts exit 0: original WITH47, transition19, interval WITH4, namespace, CTE scope, whole query binding, quoted range, duplicate targets, stored WHERE/ORDER, FK statement visibility |

Final immutable binary: `dbms_main.runtime-v3.frozen`, SHA256
`0e44e362cd5cc614621bc8c083ac3e5823bfb38145a8f60a5897e6a3cafdfc08`.
Production/header hashes match the final fresh group; no unrelated ROOT cache,
old ABI object or mutable peer object is used. The test-only expanded native
also exercises real duplicate nullable target RIDs, zero preparation reads,
OLD/NEW/source/whole stars and FK UPDATE/DELETE cascading without callback
leakage to child targets.

## Explicit remaining boundaries

This repairs the covered WITH primary FROM/USING execution root cause, not
the whole SQL/DML/WITH family. Recursive/lateral/set/group/window/function
source lowering and multi-range SELECT plans inside CTE/derived producers
remain separate work; unsupported shapes still fail explicitly. Partitioned
physical mutation locators, view/updatable source contracts, domain/usercast
catalogs and complete operator/type/collation metadata also remain partial.
No new transaction, correlated CTE restart, EXPLAIN metric, optimizer or I/O
performance completion claim is made. Ordinary non-WITH DML bridges retain
their independent contracts.
