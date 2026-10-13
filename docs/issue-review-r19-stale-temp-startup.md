# R19: discard stale session TEMP entries after, not during, persistent recovery

## Defect and correction

An abandoned `__tmp_<backend>_<name>` entry could reach persistent recovery
before startup's TEMP cleanup. Its schema might never have been durable, so
physical relation identity enumeration failed and aborted the entire database
startup. Unlogged preparation and TOAST/user-index recovery also inspected
schemas which belong to no persistent relation generation.

These four enumeration sites now skip exactly the existing numeric-backend
TEMP predicate before reading a schema. The established late cleanup still
removes TEMP table-list entries and orphaned files; it is not moved ahead of
persistent WAL recovery. Nonnumeric and incomplete numeric-looking persistent
names remain ordinary relations, and their missing schemas still fail startup.

## Verification and retained evidence

Private worktree `/tmp/dbms-review-temp-startup.GM7uTNvC/repo`, baseline
`aeaa3257`. The unchanged original `stale_temp_startup_recovery_test` aborted
with exit 134; the candidate completed with exit 0, retaining all original
survivor/name/heap assertions.

The new enumeration fixture covers missing/corrupt TEMP schemas, abandoned
TOAST and heap forks, an orphan with a large numeric backend identity, ordinary
survivors and non-TEMP similar names. Both logged-WAL and genuinely zero-WAL
UNLOGGED branches pass; the fixture verifies their actual LSN distinction.
Persistent missing-schema startup is rejected without rewriting its list or
deleting its owned marker. The accepted final fixture fails the original logged
branch (exit 1), and passes the candidate (exit 0); the old no-WAL branch passed
already and is not represented as a newly failing reproduction.

Eight whole native drivers completed with exit 0: original stale startup, new
enumeration, TEMP restart, TEMP namespace, TEMP prepared access, UNLOGGED
recovery, original UNLOGGED SIGKILL recovery and schema-marker/TEMP collision.
No original assertion or time limit was relaxed.

`r19-baseline-fresh58-normal.log` records a genuine normal baseline build of all
58 production translation units. The candidate genuinely rebuilt only
TableManage.cpp against those unchanged headers and source configuration; native
gates verified all 58 current receipts and unchanged final input signature.
Candidate Main SHA-256:
`ea3ff81558277667e0d656b1c60fc926f61956897d3bd2bf2e1711c2b3d9ec87`.
Logs in the private parent include `r19-original-baseline.log`,
`r19-final-enumeration-baseline.log`, `r19-candidate-build.log` and
`r19-final-native.log`. This is not the deferred CTAS/checkpoint专项 and does not
claim complete power-loss recovery coverage.

## Independent remaining cleanup defect (R40)

The first extended fixture also used the valid persistent name `__tmp_12_`.
TEMP filename cleanup mistook its `.stc`/fork suffix for a nonempty TEMP logical
name and deleted persistent files. The original extended fixture and all its
assertions are retained in `tests/compat/stale_temp_prefix_cleanup_known_gap.cpp`
and `r19-original-enumeration-fixture.cpp`; its baseline and first candidate
abort logs are retained. R19's registered namespace-neighbor uses `__tmp_12`
instead; it does not claim to repair the distinct R40 file-ownership defect.
That known-gap fixture must pass under a separate R40 fix before closing the
total checklist.
