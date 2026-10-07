# Cross-process transaction allocation and snapshot horizon

The process singleton loaded `.txnid` only once. A second executable could
allocate and commit after that load, while the original process continued
using the old horizon and overwrote the durable counter with its cached
next ID. The actual negative control printed `child=1 parent_horizon=0
parent=1`: the parent both omitted the externally committed transaction from
its snapshot boundary and allocated its already-used ID.

This fix serializes constructor/load, allocation, and new-horizon reads with
a persistent `.txnid.lock` `flock`. Allocation reloads the checksummed durable
state under that lock before the existing atomic write, and merges the
durable/local high-water marks without lowering either. Existing heap XID
limits, reserved failure value 0, legacy state decoder, and failed-save retry
contract remain unchanged. Snapshot-horizon reads also reload under the lock;
invalid or disappeared previously-used counter state fails closed with
structured `58030` instead of returning a stale horizon. No SQL text is used
to recover an error code.

Existing acquired `ReadView` objects are not changed. A real native reader
test warms the parent singleton, checks an external committed row, acquires
an RR snapshot, then checks that a later external allocation/commit cannot
change that snapshot's stored boundary or visible rows. After releasing that
reader, a new reader sees both commits.

## Evidence

- Permanent allocator negative: `/tmp/dbms-txn-id-owner.N3bpQF/cross-process-baseline.log`, exit 134, child 1 / parent horizon 0 / parent allocation 1.
- Independent cold-read negative: `/tmp/dbms-txn-id-owner.N3bpQF/native/snapshot.baseline.log`, exit 134, zero observed rows instead of the one committed external row.
- O2 permanent cross-process allocator and unchanged `txnid_generator_test`: `/tmp/dbms-txn-id-owner.N3bpQF/units-final.log`. The new test includes six concurrent processes making 72 allocations, exact uniqueness/high-water bounds, and corruption after warming the singleton. The unchanged test retains legacy decoding, truncation/checksum rejection, permission-induced failed-save retry, and the 32-bit tuple-XID boundary.
- Matching native nine-control proof: `/tmp/dbms-txn-id-owner.N3bpQF/native.log`, exit 0: external snapshot, six SSI shapes, shared heap rows/crash, shared B+Tree crash/owner/generation, and physical backup/restore.
- Standalone ASan/UBSan covers the full transaction-counter implementation and both pure drivers; leak checking is disabled. It is not a full-engine sanitizer result.
- Source/header/flags/object/test receipts are under `/tmp/dbms-txn-id-owner.N3bpQF/native/` and `final-inputs.sha256`. There are no public API/header/layout changes. Native engine linking uses the immutable 58-TU `620999e0` basis with a fresh O2 transaction-counter object, four O2 storage objects and matching O0 remainder, plus fresh stubs/drivers; it is not a new all-O2 build.

The separate same-parent, multiple-external-writer heap identity crash test
originally exposed this defect. Its unchanged strong row requirements must
also pass in the combined identity/transaction-counter proof before claiming
that crash matrix is complete. This item does not claim full cross-process
heap-cache/MVCC/SSI correctness, or reconstruction of a lost counter from
arbitrary old external backup/WAL history. The counter lives outside normal
database undo snapshots; these operations must not reset it.
