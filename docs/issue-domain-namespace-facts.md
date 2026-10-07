# CREATE DOMAIN must validate existing namespace facts

## Reproduction and repair

Private worktree `/tmp/dbms-domain-namespace-facts.mcOONX6M/repo` starts from
exact ROOT `11d7f82f`. Its new native test uses an actual fresh database and
native DROP SCHEMA before any catalog cache has loaded. Both unqualified and
explicit-public CREATE DOMAIN incorrectly succeeded after public was removed.
A directory named like a schema marker was correctly rejected but validation
still bootstrapped a catalog. A directly assigned malformed Session search_path
silently became public and created a domain. The unchanged baseline driver
reached all controls and exited 1 with ten failed assertions, not ten separately
proved bugs. Positive cold-public, quoted dotted namespace, username expansion
and explicit-schema-over-empty-path controls already passed.

CREATE DOMAIN now obtains a read-only set of existing catalog namespaces and
regular physical schema-marker names before validation. It no longer relies on
schemaExists' unconditional public shortcut or calls catalog get to validate.
Malformed native Session search_path raises 22023; absent selection/explicit
namespace raises 3F000. Failed validation creates no domain record, catalog
bootstrap or statement owner. CREATE FUNCTION reuses the same CPP-private
fact helper, preserving its previous first-valid explicit-path semantics.
There is no public header, file format or production source-list change.

## Actual evidence

`baseline-native-original.log` retains exit 1 on the exact 11d7 normal parent.
`build-candidate-v1-normal.log` records exit 0 with one freshly compiled O2
DdlExecutor and 57 explicitly proved matching normal parent objects. Relative
header bytes, actual flags, all unchanged source bytes and original 58 object
receipts are checked by `verify-candidate-v1.sh`. This is not a fresh all-58 build.
The candidate frozen binary SHA256 is
`4d2b7cc760981d906b75f611188240eefb035b2526b7b38d59927dd810e09301`.
The later wording-only source comment does not alter executable code; later ROOT
source changes still require their own matching compilation rather than reusing
this old public-header epoch.

`candidate-v1-native-fourteen.log`, session 37463, actually exits 0 for all 14
drivers: the new strong native, domain I/O, routine creation path/cold/declaration,
function provider, domain ancestry/default snapshot/default origin/FK, catalog
publication, no-effect image, image aliases and unchanged complete ddl_ast_bridge.

`candidate-v1-whole-nine.log`, session 69919, actually exits 1: eight complete
original scripts pass and domain_ancestry_protocol_e2e_test times out at its
unchanged default 15-second deadline. The ordinary domain FK/default lifecycle/
transaction, routine creation/search_path/SRF-path/qualified frontend and array
signature scripts pass. No timeout, SQL or assertion was weakened. The earlier
failure is retained and no full-suite approval is inferred.

## Remaining boundaries

This fixes cold declaration validation, not the general native DROP/catalog
namespace architecture. A previously loaded stale catalog still needs a separate
DROP synchronization repair; catalog bootstrap, general schema marker encoding,
temporary creation namespaces and all original SQL-05/CAT-09/TYPE-19 requirements
are not declared complete. The helper preserves the prior pg_catalog/pg_temp
creation-path exclusions and does not claim to implement their full semantics.
No push, Actions enablement or user-deferred security/TDE work is performed.
