# Function owner, receiver, JOIN-role and numeric follow-ups

Date: 2026-10-06. Independently reproduced fixes are local commits; the final
ROOT combination is not yet verified. The full 273-item goal remains active.

| Reproduced problem | Independent ROOT commit | Current evidence |
| --- | --- | --- |
| Local StorageEngine function evaluation committed through global g_engine, escaping caller rollback | `8fc7eeda` | Actual observer-zero native red to green; private fresh55 O0, final eight adjacent natives and six wire scripts |
| JOIN COLLATE rendered prefix syntax; unknown label accepted on empty input | `e98ce1a1` | Actual wire/native failures; final dedicated native/wire plus JOIN/self-JOIN controls, matching private API1 development objects |
| JOIN EXTRACT field label bound as a source column, including uppercase form | `53d33158` | Actual wire/native and retained intermediate uppercase failure; dedicated native and five protocol scripts, development objects |
| Expression helper canonicalization narrowed BIGINT/SMALLINT/REAL/DOUBLE aliases | `b661c7ec` | Actual BIGINT-to-integer native red; dedicated and four adjacent native passes; helper O2, matching other development objects |
| Resolved INT16/INT32 binary and unary arithmetic overflow was allowed | `7063fa38` | Actual native/wire red; dedicated and six adjacent natives/five wire passes; changed evaluator O2, other development objects |
| Floating arithmetic used NUMERIC/incorrect precision rather than resolved REAL/DOUBLE | `a0da0a77` | Actual REAL rounding native/wire red; dedicated and eight adjacent natives/six wire passes; changed evaluator O2, other development objects |
| Shared scalar binding changed SUM/AVG unknown-argument ambiguity from 42725 to 42883 | `adef1689` | Actual new and unchanged constraint_expr red to green; resolver/owner native and WHERE/atomicity wire controls |
| SELECT INTO eagerly evaluated later projections and side effects | `c78ea073` | Original 11 differences; final private formal O2 new42 controls plus eight adjacent scripts, ten natives, signatures/stamp |
| Indexed residual writer executed twice after post-effect heap fallback | `86ddccf9` | Permanent baseline calls2 expected1; final frozen formal-O2 candidate18 natives/nine wires; original frontend already passed, not a wire red |

Each source/test fix was committed separately. No push or Actions enablement.
The original failure expectations were preserved. Matching partial sanitizer
checks for integer/floating evaluator changes instrumented changed sources and
tests, not the whole program; they are not a full-engine sanitizer claim.

Detailed actual artifacts/boundaries are in the engine-owner, aggregate-ambiguity,
SELECT-INTO-demand and index-residual reports. JOIN/numeric handoffs and their
retained baseline/intermediate failures are under `/tmp/dbms-null-safe-join.0MiJvD`.
The metadata-only numerical overload expectations were checked against an actual
PG17.2 reference, not silently called PostgreSQL18.6 runtime evidence.

The final ROOT source was frozen at `86ddccf9`. Because owner and receiver
headers changed, terminal `96406` freshly rebuilt all 55 formal O2 production
objects and exited 0. Normal repeat build `6b41c3` reported up-to-date, and
`113e32` verified 55/55 object signatures and the binary stamp. The selected
configuration uses the TLS stub, zlib and ICU. Log:
`/tmp/dbms-index-recheck-effects.mkD1UgDL/root-owner-receiver-numeric-index-build.log`.
The previous 646 frozen combination's 30 wire passes and 23/24 native results
cannot substitute for this new combination. Its SUM failure is now independently
repaired, but the final matching constraint_expr test must still be rerun.

Frozen binary is `/tmp/dbms-owner-receiver-numeric-combination.SSCXIETb/dbms_main.frozen`,
SHA256 `a38da2abc3fb62270eedd988d67e66135a5a5f3a446f535c7e7ef92cb4a02b9c`.
Fresh matching 39 native entry points (`49782`) and 36 protocol entry points
(`96112`) are running; neither is called a completed passing group yet. The
exact arrays, fresh-stub/TU compile flags, signature checks and separate native
working directories are in that artifact directory's `verify.sh`.
The retained clause diagnostic and complete gates remain next verification.
Full default protocol/registered suite/PG18.6 differential are not newly green.
Previous full failures and statement-image I/O amplification remain recorded.

Actual remaining defects include ordinary function ORDER dispatch, subquery
error propagation, EXPLAIN execution, typed UPDATE composition, static numeric
projection OIDs, and the legacy table arithmetic bridge bypassing the typed
evaluator. Reached-statement pure metadata binding, canonical procedural scopes,
true typed parameter nodes and pre-effect collision detection remain active
independent work. The receiver does not complete every RETURNING/query shape;
the index repair does not implement all index MVCC/vacuum/SSI/heap-read failures.

All mapped families remain partial. Totals: 273 items, 22 complete, 166 partial,
70 unverified, 15 deferred_by_user. User-deferred security/TDE stays deferred.

Ledger update briefly introduced an invalid trailing comma; its validation and
six unit cases failed, and this was corrected before committing. Final coverage
validation, six ledger unit cases and the documentation/version/compatibility
checks all passed. `--require-complete` still exited 1 for actual outstanding
gaps, not a parsing error or a completion claim.
