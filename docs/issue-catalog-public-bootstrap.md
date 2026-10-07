# Preserve the actual public namespace across catalog bootstrap

`public` is an ordinary droppable namespace, not a namespace that must be
recreated at every cold catalog load. The old unconditional OID-2200 insertion
resurrected a committed DROP and duplicated a recreated public namespace with
its real allocated OID. Repeating bootstrap before first persistence also
resurrected an in-memory DROP.

## Reproduction and repair

The permanent native fixture uses actual DDL, catalog persistence, cache
eviction and independent exec. It retains fresh-database controls, rejected
DOMAIN/FUNCTION SQLSTATE 3F000 and no-artifact/owner checks, recreated namespace
identity, an actual retained table/row and bound SELECT, unchanged namespace
file bytes, standalone initialization and repeated bootstrap. An already
created public namespace must retain its identity even before first bootstrap.

The CPP-private persistence state now remembers whether namespace bootstrap
has run. The actual loaded namespace-file generation receipt distinguishes
an existing catalog from a fresh one. Only an uninitialized new catalog with
no public name receives the bootstrap public row. Other existing system
namespace bootstrap behavior is unchanged. No public header, tuple/file
format or production-TU change is made.

Evidence is retained under `/tmp/dbms-catalog-namespace-bootstrap.bKKIlCOx`:

| Evidence | Actual result |
| --- | --- |
| `baseline-original.log`, 21209 | exit 1, 11 failed assertions |
| `build-native-candidate-v1.log`, 94237 | exit 0 for the initial fixture |
| `baseline-strong-v2.log`, 51386 | exit 1, 14 failed parent assertions, including independent exec and pre-created public |
| `native-22-v2.log`, 62370 | exit 0, all 22 complete native drivers |
| `reference18-bootstrap.log` | exit 0, strict version 180006, full DROP/reconnect/recreate/OID/row matrix |

Failed assertions are not counts of independently diagnosed bugs. The original
recreated table and bound SELECT controls already passed before the fix; no
observed table-data loss is claimed. Strict PostgreSQL verification used a
uniquely created private reference database, never the shared reference public
namespace, and dropped only that owned database after closing its connections.

The matching normal O2 build compiles only the changed catalog CPP. Its actual
DDL object is from exact `a5356cd7`; the other 56 main objects are source/header/
flag/receipt-matched normal `b69bd1fc` objects. Native drivers/stubs are freshly
compiled, and all public headers, flags, 58 original receipts and 55 unchanged
native sources are checked. This is not a fresh all-58 build or approval of the
newer ROOT combination. `build-native-candidate-v2.log`, 70127, exits 0; frozen
server SHA-256:
`69c5dfd99330aaff77227f9c794c1e4b66bf0e068676b54b113d5c3f1e956c14`.

The 22 native drivers include catalog persistence/failure/clean peer/service/
resolution/view/snapshot, statement savepoint namespace, domain ancestry and
defaults, routine namespaces/providers, search_path, temp and sequence namespace
durability. The statement-savepoint fixture also printed an unchanged heap
background-writeback warning; its exit 0 is not proof of all storage correctness.

The 11 original whole scripts in `whole-11-v2.log`, 60367, are actually running
with the frozen matching binary and unchanged default disk/time limits. Their
complete terminal result is not yet known at this source commit. Earlier failed
whole runs remain retained; no deadline or SQL is removed to obtain a green gate.

## Still open

This does not turn CSV catalogs into WAL/MVCC system relations (CAT-01), replace
schema markers/physical-name encoding (CAT-09), repair all native schema/drop
owner paths, validate all legacy CSV corruption, or supply full namespace
concurrency and dependency semantics. The separate strengthened native schema
baseline still has genuine open controls. All original requirements and
user-deferred security/TDE boundaries remain unchanged. No push or Actions
enablement was performed.
