# Preserve failed FSM/VM close publications

## Independent reproduced cause

Parent source is the peer-cache repair `6f62ac73`. Failed pwrite in close
discarded the final object's only pending FSM cell/VM bit. Reopen read the old
file and the update was gone. `close-retry-baseline.log` (handle 43377) retains
actual exit 134; the baseline source/test and executable are in
`/tmp/dbms-derived-map-coherence.kTZahH6R`. This is not a cold disk successfully
persisting a failed write: no durability acknowledgement was obtained.

## Repair and ownership

Normal registry entries are weak references. Only an actual failed close
retains a strong pending cache and transfers the object's existing actual fd
to that entry. No additional fd or memory-cache allocation is needed at the
failure boundary. The fd pins `(device,inode,kind)`, preventing reuse of that
inode while the pending image exists. Reopen of the same actual inode obtains
the same pending masks; a different inode at the same filename cannot obtain
or publish the old image. A valid hard-link/renamed actual old owner may retry.

An actual successful checked file publication and exact-byte verification
release that retained owner immediately. The lookup sweep every 64 opens
removes idle weak entries and failed entries whose pinned actual inode has
`st_nlink == 0`. Missing path, rename, malformed marker, failed fstat, or retire
intent is not proof to discard pending state. This is not a permanent strong
registry for normal/clean inodes: failed entries hold only already-open fds,
so descriptor pressure remains real and new opens can fail rather than
silently forgetting prior pending state. A permanently failed still-linked
file remains retained until a checked retry or actual unlink; no invented
timeout/epoch allows discarding it.

The registry is process-owned to avoid static-destruction order allowing a
global StorageEngine to access a destroyed registry. It takes no engine,
catalog, WAL or cache mutex while holding its own lock. Callers enter it with
their cache lock; attach/sweep never take a cache lock. Explicit close still
closes the caller's handle when the pending owner transfer succeeds. Destruction
of the final old map object does not erase the transferred pending image.

File-byte formats and public map class layout are unchanged from the parent.
The shared helper header changed, so the proof uses a new full 58-CPP build,
not old ABI objects. Process exit/crash without successful persistence is
not a success: checked flush errors remain false/IO errors. This repair does
not manufacture durable data when the filesystem refuses all publication.

## Matching proof

Artifacts are under `/tmp/dbms-derived-map-close-retry.pReziFzD`.

- `build-all58.log`: all 58 production CPP fresh with shared flags plus `-O0`,
  handle 92517, exit 0; repeat up to date and all object receipts/binary stamp
  match. Binary SHA256
  `bc04902a11157bb768a4e723538c42eab54921eb1a50179b4f1aa19999137c4a`.
- `close-retry-expanded.log`: O2 actual GNU pwrite-fault control, handle 40954,
  exit 0. Original failed-close/reopen assertions remain, plus last destructor
  followed by a new owner, real fd count before/after retry, new-file/retired
  old-inode separation, and actual unlink plus 128 lookups returning fd count
  to baseline. No live map object can accidentally carry the destructor case.
- `close-retry-sanitized.log`: map CPP plus the full fault/lifetime/fd-native
  compiled with ASan/UBSan, handle 57878, actual exit 0; not whole-source ASan.
- `native-final.log`: 13 fresh natives, handle 29189, terminal exit 0: close
  retry, peer coherence, actual heap/fresh-exec, retired owner, snapshot,
  no-effect savepoint, alias savepoint, shared heap owner/rows/indexed rows,
  unchanged FK rename/reopen recovery, vacuum TOAST, corrupt-page undo/abort.
- `readonly-wire-repeat.log`: original savepoint read-only protocol whole
  matrix, handle 76149, terminal exit 0.
- `lock-savepoint-wire.log`: original DML lock/deadlock-savepoint protocol
  whole matrix, handle 37176, terminal exit 0. Both retain original SQL, rows,
  states and default 15s deadlines. Owned servers are gone.

The earlier original read-only run (handle 80448) genuinely failed at the final
DROP TABLE with the 15s socket deadline after the preceding checks passed.
`readonly-wire.log` is preserved. Later unchanged repetitions do not erase
that failure or establish controlled throughput/all latency correctness;
other root/peer load was present. No deadline, TMPDIR, SQL, or expected value
was weakened. The first peer-coherence source/artifacts remain immutable.

## Still independent

The actual cooperating-exec clean-cache flush/physicalBackup false error is a
third independently reproduced cause, not fixed here. The generic strict
raw-edit snapshot/flush negatives remain; a future consumer needs checked
cooperative publication evidence or actual safe heap reconstruction, not just
filename/inode/mtime. No TDE/security task, push or user data was touched.
