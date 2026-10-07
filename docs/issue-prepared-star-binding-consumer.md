# Prepared star expansion consumes actual output bindings

The binder already retains expanded output ordinals with actual source
occurrence/column/type identities. The prepared executor instead re-matched
qualified stars against raw range spelling, or reconstructed physical stars
from a separate storage schema. Canonical namespace aliases could therefore
lose all target columns even though metadata had resolved valid outputs.

The factory now expands each actual star site from its existing
PreparedQuery.projectionBindings. It checks the owned statement, scope depth,
source occurrence and column ordinal, then creates execution-owned ColumnRef
leaves carrying that exact binding. Qualified/USING/hidden-column decisions
remain the binder's responsibility. The original AST is not mutated. Valid
zero-column sources are not turned into an arbitrary error.

Evidence retained in `/tmp/dbms-physical-array-source-consumer.PkJ5r6tY`:

- `star-native-baseline.log` and `star-native-baseline-strong-width.log` on the
  preceding frozen library both terminate with SIGSEGV (56992/9570, exit 139)
  for a canonicalized qualified star followed by ORDER BY. The legacy factory
  produces no target leaves while retaining the declared output ordinals.
- `qualified-star-native-final.log` (28597, exit 0) passes the new exact same
  typed-source control plus pure namespace binding, original WITH logical
  duplicate-label/NULL source, ordinary scalar WHERE/ORDER, and array aliases.
  New control preserves actual three rows, two output cells, SQL NULL bitmap
  versus literal `NULL`/empty text, source close once and original AST identity.
- Normal O2 build 35993 has all 58 source/header/flag/stamp receipts, SHA256
  `c22d624d869da48ac2d353c5a41a712f9b27551030c5169a4ee97dbcb38a2a68`.

No guard was disabled and no row value determines metadata. Separate broad
array descriptor and empty RETURNING issues remain in the unchanged full wire
diagnostic; this is not an all-source/array-family completion claim.
