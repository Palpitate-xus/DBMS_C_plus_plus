# Qualified source-free routine dispatch

The legacy FROM-less path searched the projection text for `unnest(`, then
executed `UnnestOp` without preparing the actual routine identity. Consequently
`SELECT public.unnest(NULL)` returned a successful NULL row instead of 42883.
The original complete UNKNOWN-input diagnostic is retained unchanged.

Explicit qualified calls now request the existing whole-query typed consumer.
Eligibility uses the parsed call's immutable token origin: the parser's implicit
`pg_catalog` representation of SQL value keywords is not an explicit routine
qualification. All targets, predicates and row-count clauses are bound before
any target or sequence is executed. The old substring expansion is removed.
Pure routine metadata also distinguishes missing namespaces (3F000) from missing
routines in existing namespaces (42883), without folding quoted identifiers.

Evidence is retained in `/tmp/dbms-qualified-srf-host.4Sw55aIH/`:

- `prepared_srf_unknown_input_known_gap.baseline931.log`: exact original public
  routine wrong-success and partial-success assertions, immutable 931 endpoint
  SHA256 `4da42a959e4f215b5165a9f2fd381b953c6dd9530a99a3d8bf92541234d6c3ef`.
- `qualified.reference18.v1.log`: complete expanded namespace/effect matrix on
  genuine PostgreSQL 18.6 (`180006`); no timeout or state assertion relaxed.
- `qualified-v1`: fresh 58 production TUs and fresh stubs at d62, O0 after normal
  flags. `qualified-v2` rebuilds only TableManage against verified other 57;
  `qualified-v3` rebuilds only main against verified same headers/flags/other 57.
- V2 FROM-less CURRENT_USER regression is preserved in its candidate log. V3
  fixes the actual grammar-role distinction rather than excluding the test.

The expanded test creates its stored routines with the supported unqualified
declaration (actual default public namespace), and still executes the explicitly
qualified public and quoted-case positive calls. The original qualified CREATE
FUNCTION declaration's 42601 failure is preserved in baseline/candidate V1/V2
logs as a separate routine-DDL gap, not claimed repaired here.

This is not whole frontend/routine-family completion. Backend-session SRF
ownership/LIMIT 0 is a separate pending root cause. Broader SQL value keywords,
routine overload/type resolution, helper/Describe and unsupported query shapes
retain their existing open scope.
