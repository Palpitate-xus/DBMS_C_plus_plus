# Issue 975: preserve RANGE partition lower bounds

## Finding

`ATTACH PARTITION ... FOR VALUES FROM (lower) TO (upper)` retained only the upper endpoint in `TableSchema::rangePartitions`. Routing then treated every attached range as beginning at the preceding partition's upper endpoint (or at `MINVALUE` for the first range). For example, an attached `[10, 20)` accepted key `5`; the engine also accepted an overlapping `[15, 25)` range. This could route writes to the wrong partition and make the partition map ambiguous.

The regression was run against the old implementation first: `partition_test` aborted at the new assertion because the value below the attached lower bound was accepted.

## Change

Source/test commit `5ac83dae` makes `TableSchema` keep lower endpoints aligned with RANGE partitions. The schema writer appends an `RLB1` extension while leaving the legacy schema prefix intact; the reader accepts pre-extension schemas by inferring `MINVALUE` for the first lower endpoint and the previous upper endpoint for later partitions. Attach rejects intersecting half-open intervals but permits adjacent intervals; detach removes both endpoints. DDL-created `PARTITION OF ... FROM ... TO ...` bounds and table-clone paths preserve or clear the new metadata as appropriate.

This fixes explicit RANGE endpoint loss only. It does not validate or move rows already in a DEFAULT partition when a range is attached, implement partition constraint proofs or concurrent detach/finalize, or provide partition indexes and cross-partition uniqueness. Old schemas cannot recover explicit lower endpoints that an older binary never persisted; they use the historical inferred-contiguous interpretation. CAT-11 remains partial.

## Regression and verification

`tests/partition_test.cpp` covers incorrect routing below the lower bound, the exclusive upper endpoint, overlap rejection, adjacency, and reading/routing from the persisted bounds after reopening a `StorageEngine`. `tests/create_table_options_test.cpp` covers DDL-created disjoint ranges and gap routing.

The focused partition test passed:

```text
bash scripts/build_one_test.sh partition_test
```

The final registered C++/protocol/E2E suite passed on the committed source/test tree:

```text
DBMS_PROTOCOL_TEST_TIMEOUT=120 DBMS_PROTOCOL_STARTUP_TIMEOUT=120 DBMS_PROTOCOL_SHUTDOWN_TIMEOUT=120 bash scripts/build_tests.sh
exit 0; All tests passed
```

No PostgreSQL 18.6 direct oracle result is claimed: the local PG18 server rejected passwordless access and no usable credential was available in this environment. No push was performed; GitHub Actions remain disabled as requested.
