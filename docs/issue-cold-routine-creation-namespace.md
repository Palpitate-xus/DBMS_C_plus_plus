# Cold native scalar routine declaration namespace

A newly created native database already has a real regular `.schema_public`
marker but no loaded CatalogService cache. Exact ROOT `c465f898` rejects
`CREATE FUNCTION cold_implicit() ...` with 3F000, so the original
`ddl_ast_bridge` routine tests abort before reaching later assertions.

The permanent native control reproduces that exact baseline with its matching
normal-O2 production objects: `cold-baseline-original.log` terminates 134,
printing the real-marker precondition and actual state 3F000.

Declaration validation now accepts either actual catalog namespace metadata
or the engine's existing regular-file namespace markers. It does not use
`schemaExists(public)`'s unconditional assumption, initialize a catalog just to
manufacture public, or assume a dropped/missing marker exists. This is the same
namespace fact needed by cold native declaration, not arbitrary SQL name
concatenation.

The new native checks implicit and explicit public creation, an absent schema,
real DROP of an empty public namespace, and a non-file marker. Actual CREATE
may eventually publish a real pg_proc catalog; the test deliberately does not
forbid that future catalog implementation merely to keep an empty cache.
The original cold precondition and real failed declaration are unchanged.

Evidence is under `/tmp/dbms-routine-creation-path.1pbTosJh`:

- `build-cold-normal.log`: one freshly compiled O2 DdlExecutor and 57 original
  parent production objects, with every relative header, unchanged source,
  actual donor signature and flags checked. This is not a fresh all58 epoch.
- Frozen binary `dbms_main.cold-namespace.frozen`, SHA256
  `6ce9181fa3dd0b2e6b801fe9a7a2c02f64845c9b246fa7cade0e0b0654d22f50`.
- `cold-final-native.log`: the final permanent driver terminates 0.
- `cold-native-ten.log`: nine matching native controls terminate 0, including
  the new control and all eight namespace/provider/catalog/domain-FK neighbors.
  The unchanged full ddl_ast_bridge advances past the former cold failure,
  but later terminates 134 on its separate corrupt-domain storage case:
  a 58030 exception escapes its documented bool-error expectation. The whole
  ten-driver group therefore remains exit1 (9 pass / 1 fail), not green.

The unqualified non-public creation-path bug is still independently required:
its original strict180006 whole fixture passes, while the matching parent
native has thirteen failed assertions and the whole parent fixture has the
first genuine qualified lookup failure plus aborted-block cascades.
The earlier attempted path-only candidate and its two default15/disk protocol
timeouts remain recorded; they are not relabeled as this cold repair.

No public layout changes, full-family/full-suite/TLS/sanitizer approval,
permission/security/TDE audit, push or Actions activation are implied.
