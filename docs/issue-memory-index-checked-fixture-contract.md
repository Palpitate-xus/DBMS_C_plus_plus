# Missing memory-index fixture uses the checked failure contract

Original immutablefd/23a canonical native gate fails
`missing_memory_index_guard_test.cpp:46` after a legitimate `ok=false` result.
The fixture expected executePlanChecked to throw automatically rather than
checking its explicit failure result.

Hash and Bloom bitmap AND/OR paths now require failed `ok`, originalXX001
SQLSTATE, preserved original exception/nonempty message, and no partial raw,
structured or NULL rows; `throwIfFailed()` then proves the original DbError
contract. Original missing getters/files, no silent recreation, exact heap
row count, write rejection, REINDEX recovery and zero-lock assertions remain.

Actual fresh normalO2 native against byte-identical ROOT4cf production
source/header basis and all58 audited normal object signatures passes in
`/tmp/dbms-checked-native-fixtures.xmBivqmc/checked-fixtures.log`. Fresh stubs,
no API/header change. The combined run remains exit1 due to the independent
materialized-view DML session-context bug. This separate local fixture commit
does not claim a source fix, full-suite success or missing-index family closure.
