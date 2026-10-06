# Prepared namespaces and projection/ORDER follow-ups

Date: 2026-10-06. Five independent source/test fixes are committed locally.
The broader audit is still incomplete; mapped families remain partial.

| Actual defect | ROOT commit | Independent evidence and repair |
| --- | --- | --- |
| Reached PL/pgSQL queries substituted ambiguous local/column names and could execute a writing CTE/sequence before rejecting ambiguity | `272bc46f` | Pure copied catalog metadata, whole reached-query namespace preparation, owned AST/provenance and typed parameter cells; original seven defects and expanded 43 cases plus two nontransactional sequence controls verified privately. The dispatcher still uses a post-preparation legacy adapter. |
| Canonical quoted source aliases were folded again; valid qualifiers failed and invalid ones succeeded; qualified WHERE/HAVING lost rows or filters | `2a2a8d62` | Decode raw source/range identifiers once, retain exact AST components and physical column ordinals, validate before effects and lower only structurally bound physical keys/comparisons. Expanded private range wire, three natives and seven adjacent wire entries passed. |
| New scalar ORDER binding misclassified existing aggregate calls | `3983dccb` | The original registered typed-group gate passed on old formal b418 and failed on new formal f30 with COUNT 42883. Preserve aggregate-result descriptor sorting, bind argument/FILTER scalar calls without invoking them, and distinguish actual public stored COUNT. Original and dedicated gates, six natives and six adjacent scripts passed on matching private fresh-56 development objects. |
| Plain table EXTRACT consumed its grammar field as a column value and mishandled quoted/nullable source expressions | `49a3416c` | Retain original parser EXTRACT field/value roles and complete operand SQL; nullable typed actual-owner evaluation and structured error propagation. Dedicated native/wire, seven adjacent natives and eight adjacent wires passed privately. |
| Statement Describe lost CHAR/VARCHAR projection cast lengths | `2ec07854` | Derive character modifiers from the original live projection AST before the physical SELECT-star descriptor adapter; original VARCHAR(4) expected 8, actual -1 baseline retained. Independent index-only Network source passed all eight original Describe/no-Execute controls. |

## ROOT formal build checkpoints

ROOT `272bc46f` completed all **56** production translation units under the
normal optimized build configuration (`33133`, terminal 0), including the new
query_binding TU and all consumers of changed AST/RowContext headers.
Normal `scripts/build.sh` repeat reported up-to-date. All 56 object signatures
and the production binary stamp matched (`c65430`, terminal 0).
Frozen executable:
`/tmp/dbms-prepared-query-namespace-combination.FS4egSBy/dbms_main.binder.frozen`,
SHA-256 `a0a555ffbf40b554ccf6934a9cc4454bfed0f200624dd881fecb18fede9944e0`.
This is production-build evidence, not a native/protocol/full-suite pass.

The four following CPP-only fixes are integrated at production HEAD
`2ec07854`, with no new header/layout change. Matching optimized rebuild of
changed main, storage, expression helper and network TUs is running in
`25468`; its unchanged objects derive from the fresh-56 build above, not the
old 55-object group. This revision's signature audit, frozen executable and
fresh linked native/protocol results are pending. Neither the private O0/
selected-TU O2 evidence nor older ROOT green binaries substitute for them.

## Retained incomplete work

The earlier f30 full-default protocol `56926` failed during initial connect,
before SQL; its cause remains undetermined because server output was discarded.
Earlier protocol timeouts and measured transaction-image I/O amplification are
not erased by any historical green rerun. Original clause diagnostic remains
five genuine failures: WHERE/ORDER scalar subquery error propagation, two
EXPLAIN ANALYZE execution errors, and typed computed-predicate UPDATE.

Quoted-column prepared Describe still has original OID/identity/attribute
failures; character typmods are a distinct repair, not that whole family.
Aggregate arithmetic empty-input singleton/NULL, all-NULL SUM, ignored WHERE
and casted-aggregate errors remain. Hidden/compound aggregate ORDER, complete
operator/type/coercion preparation, general record/polymorphic/LATERAL scopes,
and schema-qualified routine CREATE remain independently open. The optional
binder open-namespace diagnostic retains its 44th case failure; it is not
counted as a passing 44-case group.

Detailed evidence: `issue-plpgsql-query-binding-preflight.md`,
`issue-canonical-source-range-scope.md`, `issue-aggregate-order-role.md`, and
`issue-order-metadata-arithmetic-combination.md`. EXTRACT private optimized
changed-TU evidence uses `/tmp/dbms-null-safe-join.0MiJvD`, binary SHA-256
`f02ebf02ec427d49784506ecfb414f216aa6949c976b6963a31150df48f255ea`;
the rest of that independent development core was not all O2. Typmod/private
AST artifacts are under `/tmp/dbms-parsed-expression-type.La3lmoEv`;
the typmod commit itself does not depend on the pending pure-AST helper API.

Totals remain 273: 22 complete, 166 partial, 70 unverified, 15 user-deferred.
No push, Actions remain disabled, skipped security/TDE work remains deferred.
