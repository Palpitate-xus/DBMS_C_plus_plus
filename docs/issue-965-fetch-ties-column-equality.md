# Issue 965 — compare FETCH WITH TIES using the ORDER BY column type

## Finding

Issue 964 initially compared every non-numeric peer key as collated text. That does not match typed sort equality for fixed-width `CHAR`: PostgreSQL treats values that differ only by trailing blank padding as peers. The first full regression run also exposed one older E2E assertion that still expected ordinary SELECT `WITH TIES` to be unsupported.

## Change

Commit `aa27818d` retains the ORDER BY column's type and collation metadata and uses the storage engine's typed equality semantics for peer checks. Enum keys use their declared labels. If a type cannot be compared safely, the query now fails closed with `0A000` instead of silently dropping tied rows. The regression includes `CHAR(3)` values `'a'` and `'a '` and the stale FETCH-boundary expectation now checks the valid result.

## Verification

- `bash scripts/build.sh` — passed.
- `DBMS_PROTOCOL_TEST_TIMEOUT=120 python3 tests/set_operation_structured_protocol_e2e_test.py` — passed, including CHAR padding peers, numeric equality, NULL peers, OFFSET, aliases and multi-key ordering.
- `DBMS_PROTOCOL_TEST_TIMEOUT=120 python3 tests/fetch_clause_boundary_e2e_test.py` — passed.
- Python compilation and `git diff --check` — passed.
- Full `DBMS_PROTOCOL_TEST_TIMEOUT=120 DBMS_PROTOCOL_STARTUP_TIMEOUT=120 DBMS_PROTOCOL_SHUTDOWN_TIMEOUT=120 bash scripts/build_tests.sh` rerun against `aa27818d` — passed: all 460 C++ tests and 197 E2E/protocol entries, exit 0. The preceding run on `bec759db` completed all 460 C++ tests and all but one E2E/protocol entry; the sole failure was the stale `0A000` assertion corrected here.
- PostgreSQL 18.6 differential for `set_operation_precedence.sql` — `cases=1 failed=0`, using a fresh `en_US.utf8` reference database (`server_version_num=180006`).
- A broader 18.6 differential attempt was not complete: the local server timed out on `DROP TABLE "Diff.Seq.Owned".owner_table` in `quoted_sequence_dot_schema_owned`. The three `quoted_sequence_dot_schema*` cases passed together on a fresh local server, and the failing case passed alone; this does not turn the interrupted full run into a pass.

QRY-06 remains `partial`; this narrows one peer-equality gap and does not close broader query compatibility. No push was performed.
