# Aggregate ORDER keys must retain their executor role

## Reproduced regression

The independent ORDER/API2 collector introduced in `093cf23c` sent every
non-column/non-literal function key through the row-scalar resolver. An
aggregate such as `count(*)` therefore failed with 42883 before the existing
aggregate result sorter could consume its output descriptor.

The unchanged, already-registered `typed_group_key_protocol_e2e_test.py`
provided an actual optimized before/after control:

- Production `666bf` frozen binary
  `/tmp/dbms-static-type-quoted-combination.eMhU7n4T/dbms_main.frozen`, SHA-256
  `b418a16758bafa1de3d759cd81f4f020add8c537c3ca91339133ab672d91eb5e`:
  `25836`, terminal 0, including GROUPING SETS sorted by `count(*)`.
- Production `a00` frozen binary
  `/tmp/dbms-order-metadata-arithmetic-combination.zJLGTGV3/dbms_main.frozen`,
  SHA-256 `f30d15ff4ae72e1ab2bfbf66c8cf5a3f069d4640151861d29ed76378eb0e2ad5`:
  `35286`, terminal 1 at that same query, 42883 `function does not exist: count`.

This is a real regression, not a claim that the aggregate gate was always
unsupported. The new dedicated regression also failed on its first ordinary
grouped COUNT key (`12939`, terminal 1). An earlier fixture created a public
scalar function named `count` before that control, changing the failure into
22003 (`74418`); the fixture was corrected to create it only for the separate
explicit-public-scalar control, without changing aggregate expectations.

## Repair

SELECT-list and ORDER collectors now share the frontend's existing aggregate
role set. A root aggregate call stays on the aggregate result-descriptor
sorting path, rather than being registered as a row-scalar callback. Its
argument and FILTER expressions still undergo metadata-only scalar-function
binding, so unknown callees in empty-input or lazy branches fail before a
writing sort expression can execute. No routine is invoked by preparation.

Aggregate role recognition decodes function/schema spelling once and
distinguishes canonical case and namespace. Explicit `public.count(id)`
continues to bind its actual stored function; it does not acquire the
pg_catalog aggregate role merely from its name. Window calls remain distinct.
The fix neither rolls back stored ORDER execution nor changes an API/layout.

## Matching development validation

Private candidate artifacts: `/tmp/dbms-aggregate-order-role.2PuL3N`.
All **56** production translation units and test stubs freshly rebuilt with
the combined `272bc46f` prepared-AST headers, independent canonical-range
fix, and this main-only change (`56003`, terminal 0). Before/after source and
header hashes were audited; no old 55-object ABI group was used. Candidate
binary SHA-256:
`a446ddd4caebdba7ea6efdebdb7106b6f05e22bbb8f0a4fe310d71cb8d0af4bf`.
This is a matching development/O0 build, not the ROOT optimized combination.

The unchanged original typed-group gate passed (`36642`, terminal 0), and
the dedicated aggregate ORDER regression passed (`43005`, terminal 0):
COUNT, DISTINCT, FILTER, aliases, SUM/MIN/MAX/AVG ordering, explicit-public
stored-function execution, case-sensitive unknown calls, and preparation
zero-effects. Six fresh-linked native tests passed (`20657`, terminal 0):
query binding, routine metadata namespace, ORDER metadata, scalar resolver,
constraint expressions, and statement atomicity.

Six adjacent protocol scripts passed (`16224`, terminal 0): stored ORDER,
stored WHERE, function atomicity, grouped DISTINCT/FILTER, quoted range
scope, and PL/pgSQL query binding (43 cases plus two sequence pre-effect
controls). Together with the two focused gates this is eight protocol
scripts, not eight entire compatibility families.

The unchanged full clause diagnostic still exited 1 with its original five
failures (`32762`). Scalar-subquery execution, EXPLAIN ANALYZE, typed UPDATE,
hidden aggregate sort targets, compound aggregate/window sort expressions,
complete aggregate overload/type binding, and broader query families are
not claimed fixed by this role correction. Related broad families remain
partial, and ROOT integration still needs its matching optimized validation.
