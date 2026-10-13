# R16: retain complete relation metadata on catalog read failures

## Scope and cause

Base: `b54cc7f06f211cbc6c2e020d155ee0bd3bec7cce` (2026-10-13 review).
This change concerns ordinary catalog persistence/reload correctness. It does
not change access control, encryption, CTAS, or background checkpoint behavior.

`CatalogManager::loadAllLocked` already saves the previous rows and lookup
indexes and restores them when an exception is raised. However, class and
attribute prefix parsing ignored extraction/delimiter failures; incomplete
extension fields used `continue`, dropping the row. An existing directory at
the catalog file path could also look like a successfully read empty catalog.
These paths did not throw, so the incomplete replacement became the new cache
and baseline. A later `persistAll` could publish the missing-row model.

## Change

- Check every class/attribute prefix field and separator, including initialized
  Boolean flags, and require either the complete legacy prefix or a complete
  current extension with no extra suffix.
- Preserve quoted names, backslash escapes, quoted commas, and legacy unquoted
  names without consuming the next field's first byte.
- Raise structured `XX001` for incomplete relation records, rather than skipping
  rows. The existing whole-model exception rollback retains all old rows/indexes.
- Raise `58030` for a non-file, failed open/read, or a previously observed image
  that disappears. A new catalog's legitimately absent images remain valid.
- Leave the rejected bytes/directory in place. A failed peer refresh cannot
  overwrite them; after restoration, a complete reload/publication can retry.

The change is limited to `pg_class` and `pg_attribute`; it does not claim a
complete rewrite of the other catalog CSV readers or multi-file atomic commit.
There is no new public header or ABI change in this commit.

## Actual verification

The new normal native fixture starts from catalog rows created and persisted
through `CatalogManager`. It tests every CSV field boundary of those real images
(29 class and 19 attribute delimiters), warm-cache reload, fresh read-only
snapshot, cache/byte preservation, failed publication, and healthy retry. It
also tests local directory replacement and complete legacy prefixes with
quoted commas/backslashes/quotes. It does not use syscall interception or
security/exploit instrumentation.

| Exact unchanged fixture | Original production | Candidate |
| --- | --- | --- |
| `catalog_complete_read_test` (691 checks) | exit 1, 246 failures | exit 0, no failures |
| `catalog_clean_publication_test` | not newly rerun against original | exit 0 |
| `catalog_resolve_test` | not newly rerun against original | exit 0 |
| `catalog_persistence_failure_test` | not newly rerun against original | exit 0 |
| `type_catalog_descriptor_persistence_test` | not newly rerun against original | exit 0 |

All object files used for these gates were freshly compiled from
`catalog.cpp`, `oid.cpp`, and `systables.cpp` with
`-std=c++17 -O2 -pthread -Wall -Wextra`. No Root object/cache was reused.
For the final original comparison, `catalog.cpp` was freshly compiled from the
frozen donor snapshot after byte comparison with `git show b54cc7f0:...`; the
unchanged local headers, OID/systable sources, and same new fixture were used.
An earlier four-boundary draft also demonstrated 20 original failures / 65
checks; the final full 691-check comparison supersedes that smaller draft.

Artifacts: `/tmp/dbms-review-catalog-oct13.ZlX4CjTC/`:
`r16-baseline-full48-native.log`, `catalog_complete_read_test.run.log`, and
the four original-neighbor `*.run.log` files. Logs retain the original red run.

Reproduce from a clean private worktree, with the output path replaced by a
new task-owned directory:

```sh
g++ -std=c++17 -O2 -pthread -Wall -Wextra -Isrc -Isrc/interfaces \
  tests/catalog_complete_read_test.cpp src/catalog/catalog.cpp \
  src/catalog/oid.cpp src/catalog/systables.cpp \
  -o /task-owned-directory/catalog_complete_read_test
/task-owned-directory/catalog_complete_read_test
```

The standard standalone-test driver automatically discovers the new
`*_test.cpp` fixture. These focused gates do not replace the full Root SQL/native
regression or prove the separate owner-atomicity retry issue fixed.
