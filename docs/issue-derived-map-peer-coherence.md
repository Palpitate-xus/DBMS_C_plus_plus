# FSM/VM actual-file peer coherence

## Reproduced cause

Baseline is ROOT `6ec77f02` in the independent frozen checkout
`/tmp/dbms-derived-map-coherence.kTZahH6R/baseline-repo`.
Every map object retained a separate whole-file cache. An unflushed FSM entry
was invisible to another opener; two flushes overwrote disjoint entries. VM
objects similarly lost different bits in the same byte, and a reader retained
AllVisible after another engine's real heap INSERT invalidated the page.

The checked-in native tests retain separate immediate-read, durable reopen,
actual-heap and fresh-exec controls. Baseline evidence is retained:

- `peer-baseline-first.log`: immediate peer FSM read failed, exit 134.
- `peer-baseline-durable.log`: reopen lost the first FSM entry, exit 134.
- `peer-baseline-vm-durable.log`: reopen lost the first VM bit, exit 134.
- `engine-baseline-red.log`: actual StorageEngine DML/heap FSM disagreement,
  handle 57892, exit 134.
- `engine-baseline-vm-final.log`: real INSERT leaves another engine's VM true,
  handle 28013, exit 134. Actual heap percent 99, reader FSM 255, reader VM 1.
- `engine-baseline-committed-final.log`: an existing, already committed heap
  page changes free percent 92 to 86, but reader FSM stays 92 and VM stays 1,
  handle 82249, exit 134. The seed map is durably flushed before the reader
  opens; its AllVisible setup is for an actual committed tuple, not an absent
  page.

Baseline native engine objects are all 57 non-main CPP compiled fresh against
the frozen baseline headers. An attempted older alias donor was rejected:
seven public headers differ. No such object was linked into the final baseline.
The first engine test compilation lacked PageAllocator's explicit include;
that test-only compile failure is retained in `native-first.log` and
`engine-baseline-all57.log`. The new committed-page fixture initially attempted
to recreate an already existing database (`engine-expanded.log`, exit 134);
only its setup was corrected. Neither error is counted as a DBMS defect.

## Repair

The memory cache and pending updates are shared using actual opened
`(device,inode,map-kind)` identity. Each object keeps its own checked current
path/fd ownership. Weak registry entries are pruned; an old inode cannot alias
a new inode while an actual map fd remains open. Registry lookup never holds
its mutex while taking a cache mutex or performing file publication.

FSM tracks changed cells; VM tracks changed bits. Publication holds the shared
cache mutex, obtains the actual fd's advisory exclusive flock, reads the actual
current file, overlays only pending cells/bits, then performs complete pwrite,
truncate, checked fsync and exact-byte verification. Pending state is cleared
only after success. This also prevents an independent cooperating process from
losing disjoint pending bits by publishing its own old whole-file cache.

Reads check real fd/path ownership and refresh actual changed bytes before
overlaying live pending updates. A failed read returns an unknown FSM hint or
conservative false VM result. A failed refresh does not drop a caller's new
pending cell if its actual owner is still valid. Snapshot proof remains dirty
state plus an exact actual-file byte comparison; it is not licensed by an
epoch, name or registry entry. Retired-file, symlink, dirty and fsync negatives
remain unchanged. File formats and ordinary heap visibility rules are unchanged.

## Matching proof

Artifacts and logs are under `/tmp/dbms-derived-map-coherence.kTZahH6R`.

- Whole 58 CPP with the new public map layout, shared build flags plus `-O0`:
  handle 77393, exit 0; repeat is up to date, all 58 object receipts and binary
  stamp match. `build-all58.log`; binary SHA256
  `86db5361cfb52cd2ae5abf92e00b7c671d1d48690264d6a4742030f2f4b97d58`.
- `native-final.log`, handle 81921, exit 0: nine natives (peer, engine, retired
  owner, snapshot, no-effect image, alias image, heap owner, heap rows and
  indexed heap rows).
- `engine-final.log`, handle 10061, exit 0: strengthened committed-page,
  rollback and fresh-exec engine matrix. Actual 92 to 86 is observed as 86,
  AllVisible false; all original pending-frame controls remain.
- `peer-final.log`, handle 88045, exit 0: O2 genuine-file peer, same-byte bit,
  threaded writer, hard-link identity, independent exec publication and cold
  exec verification controls.
- `snapshot-fault` control, handle 66869, exit 0: original explicit fsync
  failure does not make a dirty map quiescent and the original clean/raw-edit,
  retired-owner and symlink controls remain.
- `peer-sanitized-final.log`, handle 21829, exit 0: two map CPP plus their native
  test built with ASan/UBSan, including the independent-exec controls. This is
  scoped sanitizer proof, not a whole-source sanitizer claim.

Two unchanged savepoint protocol scripts (`savepoint_read_only` and
`dml_lock_error_savepoint`), handle 74752, have terminal exit 0, retaining the
default 15-second deadline and all SQL/state/rows assertions. Their logs are
`readonly-wire.log` and `lock-savepoint-wire.log`; their owned servers are gone.

The additional five-original-storage-native group (handle 43614) subsequently
reached actual terminal exit 0: `vacuum_toast`, unchanged 800-row
`parallel_vacuum`, `vacuum_full`, unchanged `foreign_key_action_dml` (table rename
and reopen/recovery), and `savepoint_insert_failure`. `native-storage.log`
retains all assertions and intentional corrupt-page/abort negatives. During
the run the live process's file I/O grew and was sampled in
`jbd2_log_wait_commit`; that observation is not a universal I/O root cause.
No process or assertion was stopped/modified to produce this terminal result.

Source commit is `6f62ac73865049183c3cb332c2069f0bfc5072d5` (the terminal
extension is documentation only). The all-58/source/header/flags immutable
donor is `final-immutable/`, including source.tar and the matching binary.
Its SHA is the one above. Neither the source commit nor immutable donor is
changed by the following close-lifetime worktree.

## Boundaries

This closes the reproduced actual-file cache/pending merge cause, not all
storage correctness. Advisory flock coordinates cooperating map publishers,
not arbitrary raw external writers/rename races or independently modified
physical heaps. Cross-process unflushed memory is not a durable commit. Existing
StorageEngine checked flush failures remain errors, not successful durability.

A separate real fault is retained in `close-retry-baseline.log`, handle 43377,
exit 134: failing pwrite during explicit close can discard the last pending
update. That close-lifetime root cause is the next independent repair and is
not claimed fixed here. No TDE/security or live user data was touched; no push,
deadline, assertion or data-directory substitution was used for this proof.
