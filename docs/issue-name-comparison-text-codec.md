# NAME comparison retains text semantics and implicit C collation

The shared comparison helper previously sent NAME/NAME and NAME/TEXT pairs
through its numeric fallback. Thus names `1` and `01` compared equal. NAME is
now included in the typed textual path. Without a supplied expression/source
collation, a NAME operand selects its implicit C collation; it must not inherit
the helper's default en_US text locale. Existing supplied collations still win.

This is an independent narrow comparator repair, not complete NAME, custom
operator or collation-family parity. In particular it does not implement the
full catalog operator/cast resolver.

Evidence is retained in `/tmp/dbms-quantified-comparison.oUtteDhP`:

- `name-candidate-v5.log` versus `name-reference18.log`: six actual SQL/array,
  hash/scan and cross NAME/TEXT comparisons were true instead of false.
- `name-candidate-v7-default-c.log` versus
  `name-reference18-default-c.log`: the first textual correction exposed three
  actual implicit-C ordering mismatches (`B < a` false instead of true).
- The version-checked PostgreSQL endpoint reported `180006`, not the previous
  17.2 diagnostic oracle. Each reference probe used BEGIN/savepoints/ROLLBACK.
- `name_comparison_type_test.cpp` covers ordinary binary comparisons, the
  implicit C ordering in both cross-type directions and simple CASE. The
  quantified test retains all nine wire controls as well.

The native/wire proof is against the complete frozen quantified candidate
described in [the integration proof](issue-quantified-prepared-execution.md),
not a claim that older objects match this standalone commit's headers. ROOT
must build the combined public-header change fresh. The actual PostgreSQL
[NAME implementation](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/name.c)
uses its resolved collation, with a C-collation fast path.
