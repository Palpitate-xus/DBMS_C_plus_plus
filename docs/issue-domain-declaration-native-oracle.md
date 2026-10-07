# Original DDL fixture must test DOMAIN identity, not source print style

The original complete `ddl_bridge_routing_test.cpp` aborts134 at its
`domain.name == "route_text"` assertion on the matching current optimized
production epoch. The original c465 full also preserves that failure.
The actual DdlExecutor declaration producer deliberately persists both quoted
schema and name. The original `CREATE DOMAIN route_text AS VARCHAR(12) CHECK
(length(VALUE) <= 12)` succeeds and actual getDomain returns
`"public"."route_text"`, with base source `VARCHAR ( 12 )`.
This is not failure to create the domain or a reason to remove its namespace.

The test-only repair retains that original SQL and both original unqualified
CREATE/lookup/DROP paths. It asserts exact quoted public identity, parses the
actual schema/name and verifies the matching qualified lookup, pg_namespace
and pg_type domain identity, base varchar and typmod16. The complete original
expected `VARCHAR(12)` is parsed and compared by type/modifier/array semantics;
additional literal modifier12/non-array assertions avoid an either-spelling
condition or reliance on source whitespace. DROP must remove both qualified
sidecar identity and catalog type. All original sequence/schema/SERIAL/CTAS/
ALTER/COMMENT/negative/routine-independent routing controls remain untouched.
No production source, type spelling, input SQL, namespace choice, role logic,
public header or deadline changes.

The permanent reference-only strict PostgreSQL18.6 script retains the complete
original CREATE DOMAIN SQL in an owned unique schema inside one rolled-back
transaction. Actual catalog namespace/name/domain/base1043/typmod16, qualified
and unqualified values/NULL, duplicate42710/no-change and DROP are checked.
This verifies SQL identity/type semantics, not a nonexistent PostgreSQL C++
DomainInfo field or the project's serialized quoting convention. It is not
registered as a new project protocol test or counted in the native glob.

Evidence directory: `/tmp/dbms-domain-declaration-oracle.6nlkQ6uv`.

- Root current whole native triage49140 and old c465 full retain the complete
  unchanged first assertion failure. `corrected-original-native-baseline.log`
  prints the actual fields then retains that exact old assertion and abort134.
- `reference18.log` is terminal0, with the existing runner's strict180006
  startup/version contract and complete reference controls.
- `corrected-current-native-full.log`, actual74316, is terminal0 for11 entire
  native fixtures: original DDL routing, DDL AST, namespace facts, domain
  ancestry metadata/storage/catalog/I/O/default snapshot/default origin,
  original dollar quotes and the complete original routines. This reaches the
  original routing `[DDL-ROUTE] all passed`, not an extracted subtest.
- The final additional literal modifier/base/catalog-typmod assertions have
  their own complete original-driver run in
  `final-strengthened-original-native-full-v2.log`, using a fresh service
  lookup after DROP rather than a held catalog reference. The first strong
  run is also retained in `final-strengthened-original-native-full.log`.
  The other ten fixtures and
  every production input remain unchanged from the complete11-group evidence.
- `verify-current-native.sh` checks every production source/relative header,
  actual flags, manifest,58 receipts/cache stamp and frozen bytes against the
  completed Root d661 officialO2 epoch. Only drivers/stubs are fresh; its57
  non-main normal donor objects are not a fresh58 build or sanitizer claim.
  Frozen SHA256 is
  `29f59b89b4ff3a0ea7de901285891da6d37a90e0fa667f3aa6da15348d989675`.
- The first two external preflights return1 before compiling/running because
  the helper used the private manifest pathname when checking the donor cache
  stamp. Their empty logs remain `original-current-native-baseline.log` and
  `current-native-full.log`; the corrected preflight initializes the donor's
  real configuration. This author error is not a production defect or pass.

This closes one stale native fixture contract, not complete DOMAIN/default/
namespace/catalog MVCC families or the original273 checklist. Deferred
security/TDE work, disabled Actions and the no-push rule are unchanged.
