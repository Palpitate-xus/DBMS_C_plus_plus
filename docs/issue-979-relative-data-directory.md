# Issue 979 — relative data-directory startup path

## Finding and scope

OPS-01 groups data-directory/control-file behavior with PostgreSQL-style GUC
context, source, reload, and restart semantics. The bootstrap path already
selects an explicit `-D`/`--data-dir` or `DBMS_DATA_DIR` root before global
engine initialization, normalizes relative paths against the launch directory,
validates the cluster control file, acquires a directory lock, and only then
changes the process working directory. This issue verifies the relative-path
boundary; it does not establish complete OPS-01 compatibility.

## Change

Added a regression to `tests/data_directory_e2e_test.py`: start the server from
a separate launch directory using a relative `-D` path to an existing cluster,
authenticate to it, stop it, and assert that the same `DBMS_CONTROL` remains
and the launch directory did not receive an accidentally-created cluster.
The test-only source commit is `df0c59de` (`test(config): cover relative data
directory startup`).

## Verification

- `python3 tests/data_directory_e2e_test.py`: passed against the production
  binary; covers explicit root, launch-CWD separation, relative `-D`, lock,
  control-file identity, and existing upgrade/refusal cases.
- No production source change was needed for this path.
- No full registered suite was run specifically for this test-only change.

## Remaining work

OPS-01 remains partial. PostgreSQL-compatible GUC context/source reporting,
reload/restart behavior, and the broader operational configuration contract
still require separate inventory and behavioral tests. This issue verifies only
that selecting a relative data directory is stable across bootstrap `chdir`.
