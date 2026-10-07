# Boolean DELETE predicates retain the actual RETURNING descriptor

The thin DELETE condition-string adapter did not own literal TRUE/FALSE
predicates. Its compatibility fallback emitted DELETE 0 without any result
descriptor when FALSE excluded every row. The original full physical-origin
fixture therefore lost its required RowDescription, not merely a display tag.

The structured dispatcher now retains and executes the actual whole prepared
mutation for literal boolean predicates with RETURNING (no ONLY/USING source).
Preparation resolves the real target/return namespaces before any mutation.
The existing atomic DML unit owns writes/finish; publication occurs only after
success. The output descriptor comes from that same bound mutation even for
zero rows. RETURNING expressions are not evaluated to fabricate metadata.

Evidence under `/tmp/dbms-physical-array-source-consumer.PkJ5r6tY`:

- `source-consumer-whole-v3.log` (94285, exit 1) retains DELETE FALSE's missing
  RowDescription with the original SQL/assertions.
- `delete-bool-native-baseline.log` (36038, exit 134) proves the preceding
  structured bridge did not own the actual predicate (`handled == false`).
- `source-consumer-native-v5-final.log` (39350, exit 0) passes FALSE/TRUE/empty
  FALSE controls with real columns, declared array types, zero/one affected row
  tags, nullable cells and atomic execution. Fourteen adjacent natives pass.
- `source-consumer-wire-v5-final.log` (30845, exit 0) passes all twelve whole
  fixtures, including original empty DELETE descriptors and full transition /
  array / VIEW / quantified / SELECT INTO controls. Strict 180006 full physical
  origin and element descriptor reference logs also pass unchanged.

Normal O2 28790 matches all 58 source/header/flag receipts/stamp; SHA256
`4fc50a6a94ad488d783a0e06b2d113cc94639c2a50d6bab1a1de629429b6e64d`.
This does not claim closure of every legacy DELETE predicate, correlated child
graph, binary array codec or expression-planning-priority family.
