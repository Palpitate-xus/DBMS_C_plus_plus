# Missing B-tree fixture uses the checked failure contract

Original immutablefd/23a canonical native gate fails
`missing_btree_file_guard_test.cpp:95`: the bitmap checked executor correctly
returns a failed result, but the old fixture only accepts a thrown exception.
The precise originalXX001 requirement is not removed.

Both index/bitmap receivers now assert failed `ok`, originalXX001 structured
SQLSTATE, preserved original exception and nonempty error message, and empty
raw/structured/NULL result vectors. They then explicitly call `throwIfFailed()`
and retain the original DbError/XX001 assertion. All original missing-file,
no implicit recreation, duplicate/no partial heap effects, transaction/lock
cleanup, independent secondary-family and explicit REINDEX controls remain.

Actual fresh normalO2 native built against ROOT4cf's matching58-object/header
basis passes in `/tmp/dbms-checked-native-fixtures.xmBivqmc/checked-fixtures.log`.
The harness audits all58 signatures and byte-identical private production
sources/headers before linking; test stubs are freshly compiled. The combined
three-fixture run exits1 because the separate materialized-view source-session
bug remains; it is not relabeled green. This is an independent test consumer
commit, not an index storage repair, full-suite pass or family closure.
