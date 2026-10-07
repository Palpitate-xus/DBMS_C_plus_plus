# Retain the prepared AST for actual physical array outputs

The original physical-array metadata patches could describe declared arrays,
but ordinary direct column queries still discarded their whole prepared AST
and ran the legacy projection parser. Uppercase quoted range aliases were
rejected and array identity depended on that compatibility path.

The shared scalar-query dispatcher now recognizes genuine physical array
columns by copied source declarations and then actual ColumnRef bindings /
prepared output descriptors. It retains the same owned query and typed factory
for execution, including NULL/empty sources. The preliminary physical-table
gate only avoids unnecessary preparation of unrelated legacy queries; it does
not infer output identity from a row, a table/column label, or SQL text keywords.
Existing permission and CTE/view namespace exclusions remain in force.

Initial activation exposed further real descriptor issues, fixed independently
before this consumer was published: bpchar canonical spelling, JSON/XML catalog
identities, requested/canonical TEMP namespace matching, true prepared star
bindings, REAL/double physical catalog identities, RETURNING array modifiers,
and zero-row boolean DELETE output. Their independent commits/evidence remain
separate; no protective source/type/ordinal guard was disabled to pass a case.

Artifacts: `/tmp/dbms-physical-array-source-consumer.PkJ5r6tY`.

- Whole V1/V2/V3 failures remain in `source-consumer-whole-v{1,2,3}.log`.
  They contain actual uppercase alias, canonical pg_temp, source type mismatch,
  RETURNING modifier and missing RowDescription errors. They are not relabeled
  as passes.
- `source-consumer-wire-v5-final.log` (30845, exit 0) passes all twelve complete
  serial fixtures: unchanged original nine array shapes, original 12-shape /
  24-base array descriptors, element modifiers, expanded physical origin,
  RETURNING arrays/transitions, physical character casts, WITH transitions,
  quoted arithmetic Describe, typed VIEW triggers, quantified demand, SELECT
  INTO execution demand. Simple RowDescription plus Statement/Portal Describe,
  quoted/mixed aliases, zero/NULL rows and computed-origin negatives all remain.
- `source-consumer-native-v5-final.log` (39350, exit 0) passes fifteen actual
  matched native tests including new true codec/catalog identities, boolean
  DELETE, metadata namespace/star ownership and existing scalar/source/NULL/
  floating/RETURNING controls.
- The two full reference fixtures pass unchanged in
  `full-{element,origin}-reference18-final.log`, against the isolated XML-enabled
  en_US/libc PostgreSQL runtime with strict server_version_num 180006.
- Normal O2 28790/repeat/all 58 source/header/flag receipts/stamp pass; SHA256
  `4fc50a6a94ad488d783a0e06b2d113cc94639c2a50d6bab1a1de629429b6e64d`.
  Registration and the final immutable-copy audit follow without production
  source changes. This private source is based on f6 plus the documented ARRAY/
  VIEW/metadata fixes, not the currently changing ROOT revision.

The full physical-origin fixture is registered only now that its unsplit
expectations genuinely pass; no green-only mode was created. Still open:
non-NULL ALTER ARRAY element conversion (original XX000 is retained), arbitrary
base-array input codecs/length enforcement, binary array formats, and broader
JOIN/CTE/view descriptor or routine overload contracts. This is not closure of
all TYPE09 or all-source families, and no full default protocol/registered suite
is claimed by these targeted results.
