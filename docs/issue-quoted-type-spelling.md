# Preserve quoted type spelling during expression normalization

## Defect and repair

On the current USER/version composition, the original quoted enum native test
aborted with 42704: expression normalization changed `"public"."MixedRank"`
into `"public"."mixedrank"`, although the catalog contained the correct type.
Normalization also treated a parenthesis inside a quoted identifier as a type
modifier. These are expression-consumer defects, separate from the earlier
CREATE/DROP identifier repair.

A shared, pure spelling helper now folds only unquoted bytes and removes only
unquoted modifiers. Common-type selection and result/protocol type normalization
preserve quoted case, doubled quotes and punctuation. Catalog resolution, OID
allocation and array ownership remain with their existing callers.

## Permanent verification

- New native spelling controls cover quoted case, escaped quotes, parentheses,
  array decoration, quoted builtin-looking names, builtin aliases and UNKNOWN
  common-type selection.
- The original enum native assertions remain, with additional quoted punctuation
  and literal-bracket type names.
- A registered protocol test checks 21 controls, including distinct quoted and
  unquoted types with opposite enum order, CASE, actual catalog OID identity,
  wrong-case 42704 and exact DROP identity. PostgreSQL 18.6 (`180006`) passes all
  21; the preceding frozen project binary fails five; the repaired binary passes
  all 21. The complete original enum protocol matrix also passes on both engines.

## Build and bounded publication

The private worktree compiled its own 57 production non-main units and its own
Main with normal flags after the shared-header change (sessions 67347 and 2087,
both terminal 0). No production objects were borrowed from another worktree.
Final source/test seal:
`bd362aed432d9ea0329ee1369e420615debe7abaaba1f82b11a717b9164048ee`.
Frozen production SHA-256:
`41ecb634b5ca2387a8a86ac7a64bbb89eb050e02abd4f734c426d26e5ad00c7a`.

Session 97747 actually terminated with exit 1: 23/23 native and 24/25 protocol
drivers passed, including the unchanged full default protocol test. All 58 own
production receipts, repeat-build objects, cache, source and frozen-binary checks
passed before and after execution. Evidence is retained in
`/tmp/dbms-having-grammar.MPk64yc0/quoted-type-final-publication23-25.log`;
the reference, preceding-binary failure and process crash logs remain alongside it.

The one failing driver is the unchanged view-trigger typed-values test. A real
server trace confirms SIGSEGV after a CHECK violation, with the next transaction
accessing a stale current-session pointer. This is not a timeout and is not fixed
by quoted type normalization; it requires a separate repair and commit.

This private commit is not Root full-suite approval. Root's 786 native/Main and
443 protocol registered run remains independent. TYPE-08, the original 273-item
ledger and the overall goal are not declared complete. No push or Actions change.
