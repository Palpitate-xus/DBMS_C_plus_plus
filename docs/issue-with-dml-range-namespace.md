# WITH DML source range namespace

This independent binding repair does not implement UPDATE FROM or DELETE
USING execution. Those runtime shapes remain a separate actual-red task.

Before this change, the binder checked conflicting aliases only between JOIN
inputs. It never compared the UPDATE/DELETE target namespace with its source
namespace. It also rejected two distinct, unaliased physical relations with
the same basename even when their schema qualifiers disambiguated them.

Conflict checks now use separately canonicalized visible names and actual
physical relation identities. The same alias, logical CTE name or repeated
physical occurrence is rejected with `42712`; distinct physical relations
with nonhidden schema qualifiers retain their valid independent occurrences.
Quoted `"T"` and `"t"` remain different. An alias still hides the table name.

All evidence is retained in `/tmp/dbms-with-multisource-dml.IPrdOtwy`:

| Proof | Result |
| --- | --- |
| `namespace-reference-v1.log` | Strict isolated PostgreSQL 18.6 (`180006`) passes the complete namespace protocol. Temporary objects are session-owned; ordinary test schemas are created inside BEGIN and removed by ROLLBACK. |
| `namespace-native-baseline.log` | Fresh four-source pure binding baseline using exact `fd183ec3` binder aborts with exit 134 on the first duplicate target/source assertion. |
| `namespace-wire-baseline.log` | Frozen ROOT `fd183ec3` optimized SHA `23a0f0c631a1aa837e02396726a426e0f4faf628e6a2a89b1714af3b4eff0acc` fails six assertions: five duplicate namespaces produce `0A000`, and a valid different-schema join produces `42712`. No expectation was relaxed. |
| `namespace-native-candidate.log` | Independent fresh four-source pure binder passes; metadata callbacks only copy descriptions and do not execute queries/routines. |
| `namespace-build.log`, `namespace-candidate/*audit` | All 58 candidate production objects and matching stubs freshly compiled under effective O0; no ROOT cache/object reuse. No public header/layout/new TU changes. |
| `namespace-wire-final.log` | Ten serial protocol scripts pass: new namespace, 47 primary WITH DML, original interval WITH, CTE relation scope, query binding, quoted range scope, NULL-safe joins, duplicate UPDATE, ordinary WHERE and ORDER. |
| `namespace-native-corrected.log`, `namespace-native-query-binding.log` | Eight distinct matching native tests pass: new occurrence metadata, primary WITH binding, bound DML, prepared execution/cursor, EXPLAIN, duplicate UPDATE and query binding. |

The first native adjacency invocation (`namespace-native-final.log`) passes
the new test then stops at an incorrect harness filename; it is preserved as
an exit-1 harness failure, not counted as eight passes. The corrected results
are recorded separately. The whole private optimized/ROOT combination and
canonical suite are not established by this O0 proof.

The broader 42-control WITH FROM/USING reference (`reference-v1.log`) passes
against strict PostgreSQL 18.6, while the same frozen baseline's
`baseline-v1.log` remains red. In particular, PostgreSQL modifies a multiply
matched target only once but evaluates volatile SET projections for both join
rows in the tested plan. True nullable source rows, tuple identity, source
RETURNING, derived/correlated sources, fixed command views and rollback still
need their actual runtime implementation and independent proof; this binding
commit does not mark those families complete.
