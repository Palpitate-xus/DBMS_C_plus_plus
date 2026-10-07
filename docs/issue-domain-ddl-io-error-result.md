# CREATE DOMAIN storage I/O at the native DDL boundary

The unchanged original ddl_ast_bridge storage-error assertion requires a bool
error result when `.domains` is an actual directory. Domain ancestry validation
now reads that metadata before createDomain's status branch, so its typed58030
escaped the native bool-error boundary and aborted the test. This is independent
of the preceding cold-namespace and creation-path repairs.

The original single test function is included unchanged in an isolated driver.
Matching exact c465 normal production objects reproduce exit134 before any
source repair (`domain-io-original-baseline.log`, tool41493). Matching candidate
objects run the identical assertion to exit0 (`domain-io-original-candidate-v1.log`,
tool90198). The fixture is not corrected or weakened to allow the exception.

CREATE DOMAIN now converts only typed58030 storage I/O into its existing true
error result and diagnostic bridge. The original SQLSTATE remains in the output;
RAII still unwinds any begun DDL transaction. All other semantic/cancellation
DbErrors propagate unchanged. There is no blanket exception swallowing, ignored
I/O, fabricated catalog type, fallback success or change to other DDL commands.

The new permanent native checks the bool result and exact58030 diagnostic,
no leaked transaction or modified corrupt-directory payload, successful real
domain creation after repair, and unchanged3F000 semantic exception propagation.
Its matching O2 run and ten creation/cold/namespace/provider/catalog/FK neighbors
terminate0. The complete original ddl_ast_bridge is still running after advancing
beyond its previously failed domain case; no entire twelve-driver PASS is claimed.

Evidence under `/tmp/dbms-routine-creation-path.1pbTosJh`:

- `build-domain-io-v1-normal.log`: freshly compiled sole O2 DdlExecutor, proven57
  unchanged parent objects, matching all relative headers, flags and actual58
  donor receipts; not a fresh all58 production epoch.
- `dbms_main.domain-io-v1-namespace.frozen` SHA256:
  `93891e9f3c7039a9feff085f4eb40c92d5adc2caac3a7e88a8f91ca285a3f695`.
- `domain-io-v1-native-twelve.log`:11 drivers terminal0; whole original DDL live.
- `domain-io-v1-whole-nine.log`: unchanged serial wire group still running.
  Domain ancestry already exceeds original15 deadline; Domain/FK initial connect
  fails errno103 before SQL; SRF path has timeout/cleanup failures. Those failures
  remain, not inferred to be domain I/O semantic regressions. The unchanged
  complete creation-path and lifecycle scripts already terminate0 in this run.

Current ROOT has later domain/default and derived-map public headers and must
perform its own consistent optimized combination. No current full-suite, all
domain/function/storage family, TLS runtime or sanitizer claim follows. No push,
Actions activation or user-deferred security/TDE work is authorized.
