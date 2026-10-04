# CAT-11: fail closed when ATTACH cannot share relation storage

## Finding

The PostgreSQL 18.6 oracle confirmed that attaching an existing standalone
child preserves its relation and makes its rows visible through the parent
(`parent=1`, `child=1` after attaching a child containing one row). The local
storage model instead stores partition rows in a parent-owned
`parent#partition` fork. The child relation continues to read and write its
own `.dt` file, so a successful ATTACH could make parent and child queries
observe different data. An initially empty child has the same problem for
later direct writes. `CREATE TABLE ... PARTITION OF` used the same split path.

The full fix requires relation-to-partition storage mapping, including DML,
indexes/TOAST, and transactional rollback. That migration is not implemented.

## Change

`StorageEngine::attachPartition` now holds the parent metadata lock and a
shared lock on the candidate relation in canonical name order. The shared
child lock is compatible with an existing same-transaction DML IX lock and
stabilizes the candidate against relation DDL. After existing bound/default
constraint checks, but before creating a fork or publishing parent schema, it
returns `FEATURE_NOT_SUPPORTED` whenever the candidate is a real relation.
Storage-only synthetic partition names that are not standalone relations
remain supported.

The DDL executor maps this result to SQLSTATE `0A000` for both
`ALTER TABLE ... ATTACH PARTITION` and `CREATE TABLE ... PARTITION OF`;
the latter rolls back the just-created child. Error ordering is intentional:
an existing DEFAULT row that violates a new RANGE bound is still rejected as
`23514`, and a missing ALTER child is still rejected as `42P01`.

## Verification

- `bash scripts/build_one_test.sh create_table_options_test`: passed. Valid
  `CREATE TABLE ... PARTITION OF` requests fail with `0A000`; rollback leaves
  no child relation or parent partition metadata.
- `bash scripts/build_one_test.sh alter_table_only_test`: passed. Missing child
  returns `42P01`; populated and empty standalone children return `0A000`,
  parent metadata is unchanged, and the populated child's row remains visible
  from that relation while the parent remains empty.
- `bash scripts/build_one_test.sh partition_test`: passed. DEFAULT-row
  conflicts still return `23514`; synthetic storage partition routing remains
  available.
- `bash scripts/build.sh`: passed and linked `dbms_main`.
- PostgreSQL 18.6 oracle (`server_version_num=180006`): existing child rows are
  visible from both child and parent after successful attach. The local engine
  now reports its unsupported storage migration rather than claiming that
  behavior.
- The final registered C++/protocol/E2E suite was not rerun. An earlier
  intermediate full-suite attempt exposed the unsafe PARTITION OF path and was
  stopped; that failure is not reported as a pass.

## Remaining scope

This is a fail-closed mitigation, not PostgreSQL-compatible ATTACH support.
The genuine relation storage mapping, direct child reads/writes after attach,
child schema/constraint validation, index/TOAST migration, partitioned child
hierarchies, concurrent detach/finalize, and cross-partition uniqueness remain
unresolved. CAT-11 stays partial.

Source/test commit: `1612d142`.
