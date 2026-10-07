# Transaction-local search_path

The matching `0fdb5314` baseline rejected `SET LOCAL search_path=public`
with XX000 (`Invalid value for parameter local search_path`). The subsequent
25P02 failures in the whole SRF fixture are consequences, not separate bugs.
The independent lifecycle script retained 46 failed assertions on that baseline.

`Session::SearchPathTransaction` now owns the original, pending session-level
and currently visible values, plus ordered named-savepoint snapshots. A local
assignment changes visibility only; a regular SET/RESET changes the pending
session value too. Successful COMMIT/PREPARE, full abort and named recovery use
the corresponding real transaction callbacks. Short-rent Session copies own
their values, not pointers to a returned backend. Quoted search_path components
remain quoted; a SQL string is decoded separately from a quoted identifier.

Permanent protocol and native tests cover nested/duplicate savepoints, RELEASE,
SET followed by LOCAL, RESET, commit/rollback, failed blocks, CHAIN and standalone
LOCAL. Strict PostgreSQL 18.6 (`180006`) passes the complete protocol fixture.
RESET is checked against the actual connection's startup default, since the two
servers have different configured defaults. The initial incorrect default guess
and its reference failure remain in the external evidence directory.

Artifacts: `/tmp/dbms-provider-search-path.tD6Q3AdD/` holds the baseline build,
`guc.baseline.log`, strict references, source/header audits and matching candidate
proofs. Final V6 uses a genuinely fresh all-58-TU + stubs V5 O0 header epoch,
plus the source-audited final evaluator-only rebuild. The complete 14-script
serial protocol group, ten matching native drivers and four scoped ASan/UBSan
drivers all terminate with exit 0. The sanitizer group instruments only parser,
evaluator and DDL undo production TUs; it is not a full sanitizer/leak proof.
This is private O0 evidence, not a formal O2/full gate.
No protocol timeout, existing assertion or sequence expectation was relaxed.

This is search_path's transaction-local contract, not complete transaction-local
support for every GUC or SET configuration inside stored routines.
