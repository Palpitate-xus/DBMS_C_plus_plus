# STO-01: data-directory control format integrity

## Finding

The existing V2 `DBMS_CONTROL` recorded the system identifier and the core
format fields, but had no checksum. Replacing its identifier with another
valid 16-digit value was accepted by `--check-data-directory`, so accidental
or partial metadata damage could make one cluster appear to be another.
V2 also had no explicit feature-flags or WAL-segment-size field.

## Change

New clusters use the documented V3 format in `docs/PACKAGING.md`. It records
control/catalog/heap versions, block and WAL segment sizes, native byte order,
reserved feature flags, and the system identifier. CRC32C covers the exact
serialized metadata through the identifier. Parsing is bounded, requires a
regular non-symlink control file and final LF, checks each supported field,
rejects unknown flags/trailing data, and fails closed on checksum mismatch.
Opening is nonblocking before file type validation, so a raced FIFO cannot
hang startup. Replacements use the existing synced temporary-file, atomic
rename, and parent-directory sync helper.

Normal startup and read-only verification accept only compatible V3 files.
`--upgrade-data-directory` explicitly upgrades V1 or V2 control metadata to
V3 while preserving the system identifier; it does not migrate catalog, heap,
or index contents.

## Verification

- `./scripts/build.sh` passed.
- `python3 tests/data_directory_e2e_test.py` passed, including damaged IDs,
  checksum-preserving incompatible fields, missing LF, symlink/FIFO/oversize
  files, and V1/V2 offline upgrades.
- `python3 tests/checksum_verification_e2e_test.py` passed with a separately
  generated V3/CRC32C fixture.
- The registered `./scripts/build_tests.sh` completed with exit 0 on the
  immediately preceding parser revision: 460 C++ tests and registered
  E2E/protocol tests passed; its TLS wrapper was skipped because that build
  used the TLS stub. The final nonblocking-open/LF-framing hardening was then
  rebuilt and checked with the focused E2Es below, not another full-suite run.
  The checksum-verification E2E is separately run because it is not in that
  registry.
- `python3 tests/gap_progress_test.py`,
  `python3 tests/documentation_status_test.py`, and `git diff --check` passed.

## Remaining scope

This advances the fields named by the audit row, but does not implement the
broader blueprint's redundant control-file copies or checkpoint/timeline
metadata. WAL and checkpoints are currently maintained per database, so a
cluster-wide checkpoint/timeline control record needs a separate storage/WAL
design. Feature flags currently accept only the all-zero baseline. STO-01
therefore remains partial until those control-file design requirements are
resolved and verified.
