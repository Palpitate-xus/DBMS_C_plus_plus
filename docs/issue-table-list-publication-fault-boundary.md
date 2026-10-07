# Table-list publication fault boundary

This is an independent test-contract correction, not a relaxation of CREATE
TABLE durability. The production source, public headers, build flags and
registered whole-protocol tests are unchanged.

## Original counterexample and actual cause

The complete original `tests/table_list_atomicity_test.cpp` fails at its
`createStatus == DBStatus::OK` assertion on source `d661ca4b`, just as on
`82bf3739`. Its original source SHA-256 is
`d3c5fc496204e21a3eba7c89166c37f52006a520241e2089cbef40015ddc2ee6`.

The original `failDirectorySyncAfterForTesting(1)` was intended to allow a
heap-allocation marker publication and then fail the table-list directory
barrier. The real CREATE path now first durably allocates a physical relation
identity. A trace linked with the unmodified original test and the actual
production objects shows:

1. `.physical_relation_ids` is successfully renamed with failure count 2.
2. Its parent directory is actually synced, leaving failure count 1.
3. `<database>/.physical_relation_id_highwater` is successfully renamed with
   failure count 1; the following directory barrier is the injected failure.
4. CREATE reports `could not allocate durable physical relation identity`,
   before publishing a schema, heap or new table-list generation.

The original test therefore expects success for an actual failed precondition,
not for the intended transient table-list barrier. Changing production to
accept that failed physical-ID barrier would violate the durable identity
contract. The recorded original native exits 134; the trace verification
script itself exits 0 only because it explicitly expects that counterexample.

## Preserved and strengthened contract

The original database, table definitions, `alpha/remove_me/omega/created`
inputs, CREATE-success assertion, absence-of-allocator-flush-error assertion,
fixed-record table-name ordering, DROP-success assertion and final cleanup
remain in the test. The original `failDirectorySyncAfterForTesting(1)` also
remains as a genuine negative case. It must return the existing failure status
and exact cause, preserve every original table name and seeded row, and leave
no `created` relation or temporary table-list artifact. A complete same-name
CREATE retry (with no manual directory-sync repair) and DROP prove
recoverability without reusing an old physical ID.

A separate exact-target rename failure must reject the old table-list bytes,
clean up the partially initialized relation, and preserve all surviving rows.
The CREATE and DROP positive cases then fail their actual `tlist.lst` parent
sync only after the target's successful rename. Each must consume the one-shot
failure and complete a real successful directory-sync retry before returning.
Fresh independent engine loading verifies the surviving names and row values.

## Thread-owned fault and competing-thread control

The first candidate, `55473551`, had a verification race. Its rename hook
armed the existing global directory-sync failure counter, although its
rename/fsync bookkeeping was thread-local. Another thread could consume that
global error first. The selected thread's first successful kernel fsync was
then incorrectly counted as a retry, without ever observing its own failure.
The immutable first candidate is not accepted as proof of the positive fault.

The diagnostic `old-554-thread-consumption.cpp` preserves that candidate's
entire fixture and every assertion, adding only deterministic scheduling and
trace output at the fault hook. For both CREATE and DROP, another thread first
gets the actual global-counter error and then successfully syncs the same
directory. The selected thread's first actual kernel sync then returns 0 with
global count 0; the old complete fixture still passes. This is a reproduced
false-green verification counterexample, not a production durability failure.

The corrected positive fault never arms a global token. Successful rename of
the exact `tlist.lst` target sets a thread-local `InjectFailure` stage. The
selected thread's first fsync must actually refer to the same directory
device/inode according to `fstat`; the test wrapper returns `EIO` once and
records `injectedFailures == 1`. Only the next fsync of that same directory,
after a successful real `SYS_fsync`, records `durableRetries == 1` and clears
the stage. A file sync or another rename cannot masquerade as that retry.

Before each CREATE/DROP positive reaches its first selected-thread fsync, a
joined competing thread performs a real successful sync of the same directory.
Its own thread-local wrapper records exactly one successful kernel directory
sync, zero injected errors and zero retries. After joining, the selected
thread must still have its pending failure, with zero injected errors and
zero retries. The final positive asserts one competing-thread sync, one
selected-thread injected error, and one actual selected-thread durable retry.
The original global `failDirectorySyncAfterForTesting(1)` remains only in the
separate highwater-negative case, with its actual cause asserted.

The POSIX `rename` and `fsync` interposition lives only in this standalone test
executable and otherwise uses the real Linux syscalls. No production source,
public header, special compiler definition, linker wrapper or runner exception
is needed: the normal native-test loop executes the entire deterministic test.

## Evidence and scope

Historical artifacts remain unchanged under
`/tmp/dbms-table-list-publication.aSTiC8vW/`:

- `original-trace.log`: complete unchanged original, actual rename/fsync trace,
  native exit 134 and verification-script exit 0.
- `current-native-10.log`: all ten original native entries individually exit
  0, but the shell wrapper exits 2 with an unexpected EOF after its external
  helper was edited while still running. This is retained as a verification
  authoring failure, not counted as a passing group or a production failure.
- The previous ten-native/ six-wire/ target/ repeat logs all remain, with
  their true exit statuses. Their positive fault bookkeeping does not prove
  selected-thread injection and is not used to close that verification gap.

Current artifacts are under `/tmp/dbms-table-list-thread-fault.eG9Kixi5/`:

- `old-554-forced-thread-consumption.log`: the actual deterministic false-green
  counterexample described above, entire old fixture and wrapper exit 0.
- `final-native-10.log`: the helper and all test/production inputs stayed
  frozen for the complete ten-native group; every entry and the real wrapper
  exit 0. It includes table-list atomicity, schema-write atomicity, CREATE
  PK/FK/unique validation, multi-table DROP, FK-group DROP, DROP CASCADE,
  heap-WAL generations (five scenarios) and retirement (nine scenarios).
- `final-whole-wire-6.log`: all six complete original wire files and the real
  wrapper exit 0: DROP-list syntax, multi-table DROP, FK-group DROP, plan-cache
  invalidation, primary-key plans and missing-index recovery. Their original
  SQL and protocol deadlines are unchanged.
- `final-target-repeats.log`: three complete deterministic target executions
  in fresh directories, all exit 0, using the separately frozen executable.

All 58 real normal-O2 production receipts, source bytes, public header bytes,
manifest, flags and build stamp were validated against the immutable `d661ca4b`
production gate. It built a fresh TableManage unit plus 57 independently
source/header/flags/receipt/object-byte-proved O2 donors, not a fresh all-58
build. This test-only candidate links the unchanged 57 non-main production
objects plus freshly compiled test driver and stubs. Its actual server binary
is `/tmp/dbms-canonical-forked-archive.jVkV94dk/dbms_main.forked-archive.frozen`,
SHA-256 `29f59b89b4ff3a0ea7de901285891da6d37a90e0fa667f3aa6da15348d989675`.

The corrected target source SHA-256 is
`e4f3e44913600c2e65f10e4e49f0fc2b895c78a2f7f01e1d6745750132fdc2db`;
its frozen native executable SHA-256 is
`18f53935823cc489efb4a46389de8ca69ce587b8435d0a9e2ebd1d4c91d75496`.
The helper source SHA-256 is
`cde16e4976667daf183d5c931c3a8600df98bcb9629804a64a59023df8a10e7c`;
helper, test and production inputs stayed frozen until every run was terminal.
The broad classification of CREATE persistence failures is still OPEN: this
fixture retains the actual existing `DBStatus::INVALID_VALUE` status, whose
public mapping is `22023`, rather than mixing an independent production
I/O/SQLSTATE change into this test-only correction. The fresh independent
reader is a real second StorageEngine, not an exec-based cold-process claim.
No current full project-suite PASS, fully instrumented production sanitizer
build, PostgreSQL filesystem-fault equivalence, namespace-family completion
or closure of the original 273-item ledger is claimed. No push or GitHub
Actions activation was performed; user-deferred security/TDE work is untouched.
