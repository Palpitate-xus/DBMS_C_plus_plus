# JSON/XML physical-array catalog identities

JSON and XML declarations already publish physical array OIDs 199 and 143, but
the bootstrap catalog lacked both scalar and array pg_type identities. Pure
query preparation therefore copied an `unknown[]` source type. Once the genuine
array-column consumer reached the typed prepared executor, its descriptor guard
correctly rejected that mismatch rather than guessing a type from NULL rows.

This independent change adds the four builtin identities: JSON 114/_json 199,
XML 142/_xml 143. Arrays retain the actual scalar typelem, array category, and
variable-width length. Existing persisted catalogs receive the missing rows
through the same idempotent bootstrap as other builtin types.

Evidence retained in `/tmp/dbms-physical-array-source-consumer.PkJ5r6tY`:

- `json-xml-native-baseline.log`: frozen preceding library aborts on missing
  scalar OID 114 (46431, exit 134).
- `json-xml-reference18.log`: the isolated XML-enabled PostgreSQL runtime
  reports server_version_num 180006 and the exact four namespace/category/
  length/element identities. No reference settings were changed.
- `json-xml-native-final.log`: new catalog identity/persist/evict controls plus
  unchanged bpchar/physical-array-OID/type-alias controls pass (79738, exit 0).
- `build-json-xml-normal-O2.log`: normal O2 build, repeat, all 58 source/header/
  flag signatures and binary stamp match (64695, exit 0). Executable SHA256:
  `136a2c7cd749d670f8a73cff52a8cf8a7b11624abc03cbf8ce2282cc59f7fc7e`.

The executable also contains the separately tracked uncommitted main array
activation. Native controls do not link main.cpp. This commit does not claim
arbitrary non-NULL JSON/XML array codecs, complete pg_type array graphs, or
closure of other qualified-name/physical-origin protocol failures.
