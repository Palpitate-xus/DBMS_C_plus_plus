# Qualified source references use canonical metadata identity

The metadata producer resolves the session's `pg_temp` alias to its actual
namespace. The query binder compared the original ColumnRef schema bytes with
that canonical namespace, rejecting a legal `pg_temp.t.v` reference with
42P01 even on WHERE FALSE. Alias/canonical spellings are not distinct relations.

This independent fix resolves a differently spelled schema-qualified reference
through the existing copied-metadata relation callback and compares canonical
physical schema/name identities. Range aliases still hide the original schema;
CTE/derived ranges never acquire an invented physical identity. Quoted identifier
components are re-encoded only for the metadata name API, never executable SQL.

Evidence in `/tmp/dbms-physical-array-source-consumer.PkJ5r6tY`:

- `qualified-native-baseline.log` (66190) aborts with the actual 42P01.
- `qualified-star-native-final.log` (28597) passes the pure metadata control:
  requested/canonical TEMP namespaces, quoted columns, qualified stars, and
  negative unrelated namespaces/alias-hidden schemas. No executor is supplied.
- `source-consumer-whole-v3.log` (94285) reaches the previously failing exact
  pg_temp projection and uppercase quoted range alias with all Simple and
  Statement/Portal descriptors intact. Other broad array and RETURNING failures
  remain recorded; the whole fixture is not claimed green.
- Fully frozen normal O2 build 35993 and all 58 receipts/stamp pass; SHA256
  `c22d624d869da48ac2d353c5a41a712f9b27551030c5169a4ee97dbcb38a2a68`.
  An earlier build 25697 detected a concurrent local correction and exited 1;
  it is retained, not used as a matched proof.

The candidate also includes a separate prepared-star execution change and the
pending main array activation. The native pure binding test does not link main.
This does not claim generic runtime schema/VIEW/CTE Describe closure.
