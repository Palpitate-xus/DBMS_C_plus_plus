# Character-array catalog spelling and execution identity

The physical CHAR descriptor is canonical `character[]`, while its PostgreSQL
catalog identity is `bpchar[]`. TypeRegistry lacked the `bpchar` alias. The
prepared array-column consumer therefore rejected an otherwise genuine source
cell as a synthetic projection with an invalid positional binding. This was
not a missing-row or NULL-value inference problem.

The independent change registers `bpchar` as the existing SQL `character`
type. It does not conflate PostgreSQL's separately quoted internal `"char"`
type with SQL CHAR. The new native control checks canonical identity, a real
CHAR(3)[] source, and SQL NULL separately from literal TEXT `NULL` and empty
array elements.

Evidence is retained in `/tmp/dbms-physical-array-source-consumer.PkJ5r6tY`:

- `bpchar-native-baseline.log`: frozen preceding library aborts at the missing
  `bpchar` canonical identity (session 4419, exit 134).
- `bpchar-native-corrected-final.log`: the new control and unchanged physical-array
  builtin-OID/type-alias controls pass (session 86469, exit 0).
- Normal O2 executable build 65286 and all 58 source/header/flag receipts pass.
  Its SHA256 is `6465525640f14f5f15a703d4f83b3b78a933c41d1b6862184bc63d865be660db`.

The runtime candidate also contains an uncommitted main array-source activation
change. These native controls do not link main.cpp, so their library evidence
is independent. The complete physical-array protocol fixture still exposes
other catalog/namespace defects; this commit does not claim that fixture or
the whole array/type family passes. An initial new-fixture assertion about a
SQL NULL's opaque payload was corrected to check its actual NULL bitmap; the
original payload failure is retained rather than described as a product fix.
