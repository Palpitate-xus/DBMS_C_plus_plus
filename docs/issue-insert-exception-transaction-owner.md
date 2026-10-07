# Embedded INSERT exception ownership

Status: repaired for exceptions thrown by `insertInternal` and completion in
the public `insertRow` transaction/statement boundary. This is not a claim that
all storage exception paths or process-global registry lifetimes are repaired.

## Actual failure and fix

On source `75039dd0664eb2495456ca75902e9197b086f2a4`, an AFTER INSERT callback
inserts an audit row and throws a derived `DbError`. The error type survives,
but the implicit transaction remains active and RETURNING contains the failed
row. The unchanged strong native test reports `typed=1 returned=2 active=1`,
then aborts. Status-returning errors already used rollback; exceptions bypassed
that path entirely.

`insertRow` now catches exceptions after starting its own transaction, rolls
back, truncates only rows appended by this statement, and rethrows the original
exception. In a caller-owned transaction it rolls back/releases its internal
statement savepoint, preserving earlier writes and explicit user savepoints.
An unsuccessful statement recovery falls back to full rollback. A cleanup
exception must not replace the original exception subtype/SQLSTATE.

## Evidence

All paths below are retained local artifacts, not repository files.

| Check | Actual result |
| --- | --- |
| Exact 750 baseline headers/source/flags, frozen original 58 objects | Baseline test exit 134; typed=1, returned=2, active=1 |
| Candidate official source manifest | 57 byte-identical baseline objects + freshly compiled TableManage.cpp, all 58 signatures match; build/repeat exit 0 |
| Candidate native regression | typed=1, returned=1, active=0; caller transaction keeps its earlier row/user savepoint; audit rows absent; exit 0 |
| Six matching natives | New exception owner, AFTER/BEFORE trigger failure, damaged-savepoint insert failure, WAL failure, generated insert trigger: all exit 0 |

Directory: `/tmp/dbms-insert-exception-owner.cOdyUwvn`.
Logs: `baseline-v5-detail.log`, `candidate-build.log`, `native-final.log`,
`native-final-detail.log`. Candidate executable SHA256:
`dcfee54cbd68df1e0f3751660f54904469550e6f37c265803a76d1c520d31e08`.
These are private O0 proofs, not a current ROOT normal-O2/full-suite result.
Earlier new-test API/compile harness failures are retained and not counted as
runtime failures or passes.

A separate domain corruption control additionally exposed an exit-time ASan
use-after-free in the process-wide active transaction set after this leaked
transaction. The new owner cleanup eliminates the leak in that reproduction;
arbitrary global-engine destruction with an abandoned transaction remains a
distinct lifecycle issue, not fixed by this commit.
