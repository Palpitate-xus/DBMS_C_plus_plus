# Derived map publication and snapshot ownership

The pre-fix `FreeSpaceMap` and `VisibilityMap` kept a `std::fstream` and
unconditionally rewrote its inode on close. Replacing their pathname did not
stop a retained handle from changing the retired file. `flush()` also cleared
the dirty flag without checking whether its write/flush succeeded. That is not
a sufficient durable-cache proof for a savepoint optimization.

`derived_map_owner_test` reproduces the retired-owner write with the existing
public APIs. The old source failed its unchanged retired-file assertion
(tool 4920, exit 134). The new source passed the identical test (39027, exit 0).

The maps now use an owned descriptor. Publication requires an opened regular
inode matching the current non-symlink pathname, successful complete writes,
truncate and file fsync. Failed publication retains pending dirty state.
`flushChecked()` returns failure rather than licensing a snapshot. A clean
flush/close does not rewrite the file. `quiescentForSnapshot()` additionally
compares the entire cached byte vector to its actual current file contents.
The byte formats are unchanged; no storage migration is performed.

`derived_map_snapshot_test` covers clean/no-rewrite, dirty state, reopen,
physical byte edits, replacement ownership, closed handles and symlink
rejection. Its optional GNU fsync wrapper fails exactly one sync after the
write, verifies the pending state remains, then verifies a real successful
retry. The O2 fault build (92989) and ASan/UBSan build (85263) both exited 0.
Two earlier standalone compilation failures exposed a missing explicit
`<string>` include after removal of `<fstream>`; they were not runtime passes.

Private proof directory: `/tmp/dbms-savepoint-image-demand.9gzfRDln`.
This foundation does not by itself close the savepoint latency issue. It is
not a general multi-engine FSM/VM cache-coherence protocol, nor a guarantee
against arbitrary concurrent external filesystem mutation. The later engine
consumer must still hold database/cache ownership and compare the complete
original physical image. Public class layouts changed and require a matching
complete rebuild; old objects must not be reused across this change.
