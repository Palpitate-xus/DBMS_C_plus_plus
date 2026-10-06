# Integrated typed-query, EXPLAIN and UPDATE verification

The independent source/test integrations listed in
`docs/issue-full-registered-canonical-1e0a8c5a.md` now additionally include
ROOT `3bc45e3b` (private `c09f9d5b`), the ordinary bound typed UPDATE
repair. Nineteen independent ROOT source/test commits after 1e0a8c5a remain
separate, including two test-only contract corrections. No push occurred.

## Fresh matching ROOT production

Artifacts: `/tmp/dbms-native-explain-prepared-combination.64xbTwJu`.

- Source efc501b9 (later documentation-only 57d5c218) freshly compiled all
  57 production translation units at the normal O2 configuration. Build
  86360 exited 0 with exactly 57 compile entries. Normal repeat was
  up-to-date, and every object signature and binary configuration stamp
  matched. This was required by the changed Condition and typed-plan public
  interfaces; no old 56-source/header ABI objects were substituted.
- Frozen pre-UPDATE binary SHA256 is
  `dd3ea2eed3e4b96f062f6018d0af86c263da8b54ffe721433dc6582f9aa1f7db`.
  The unchanged clause diagnostic 3935 exited 1 with exactly three remaining
  failures: ordinary scalar WHERE, ordinary scalar ORDER, and typed UPDATE.
  Its two EXPLAIN paths passed because of the separately integrated plan
  repair, not a change to the diagnostic assertions.
- Ordinary UPDATE commit 3bc45e3b required exactly one changed DML CPP
  rebuild at normal O2 (83522, exit 0). The other 56 production units remained
  source/header-identical to the fresh group above. Normal repeat was
  up-to-date; all 57 signatures and the binary stamp matched again.
- The exact combined frozen server SHA256 is
  `09121ee818a71dc1f34309ce4a0a827dcf257885f80964f59f7733b60700b5a6`.
  Its source revision is 3bc45e3b; subsequent documentation commits do not
  change the frozen source/headers/tests/registry.

## Actual matching regressions

Focused native session **81137 exited 0: all 81 tests passed**. Every native
was freshly compiled and linked against the matching production object
group, with fresh stubs and an isolated working directory. This includes
all fifteen previously added new natives, the new typed UPDATE native,
the old full-gate five failures and extensive adjacent query/index/NULL/
constraint/transaction/DML fault-injection controls. Intentional corruption
and rollback-failure diagnostics printed by those fault tests are not
mistaken for failures: their original assertions passed.

Focused wire session **53215 exited 1: 67/68 entry points passed**. The one
failure was `table_extract_quoted_operand_protocol_e2e_test.py` during initial
TCP connect in `runner.start_ours`: ConnectionAbortedError/errno 103, before
any SQL or EXTRACT assertion. Its discarded server output does not establish
the startup cause or an EXTRACT regression. Output/connection-captured
repeat 15821 exited 0 at unchanged startup/socket deadlines: the server
started normally, initial connections reported alternating errno111/103
before succeeding, and all original EXTRACT/OID/effect controls passed.
Owned PID 4091312 finished with status 0. This does not prove the cause of
the original failure or repair startup reliability; the original failure
remains retained. This group includes the original 51 controls,
all eleven newer E2E scripts, both UPDATE matrices and adjacent command-tag/
OLD-NEW/signed-predicate controls. It is not relabeled as a complete pass.

The same combined binary's unchanged clause diagnostic 8388 exited 1 with
exactly two remaining failures: scalar WHERE swallowed P0002 and scalar
ORDER ignored 22P02. Its original typed UPDATE and two EXPLAIN controls now
pass without changing their expectations or effect/rollback assertions.

The unchanged full canonical `scripts/build_tests.sh` session 75090 is
still running against the same frozen server. Its actual inventory is
**517 native tests and 257 registered protocol/E2E entry points**, not the
old 501/244 inventory. Its initial configuration check happened before the
separately audited warm-cache helper 80470 finished writing its cache stamp.
The standard runner therefore correctly invalidated that test cache and is
freshly compiling its 56 no-main production units itself. The warm helper
exited 0, but its object reuse is not claimed as the runner's actual path;
the runner's fresh objects and original output remain authoritative. No
process was restarted to hide this extra compilation or any test result.

Logs: `build-full-O2.log`, `build-full-O2-repeat.log`,
`audit-preupdate.log`, `known-gap-preupdate.log`,
`build-typed-update-O2.log`, `build-combined-repeat.log`,
`audit-combined.log`, `warm-test-cache.log`, `focused-native.log`,
`focused-wire.log`, `known-gap-combined.log`, and `full-registered.log` in the
artifact directory.
The captured repeat is `extract-startup-captured-repeat.log`; its external
capture wrapper changes only tracing/output destinations, not repository
source, startup deadlines or test expectations.

## Remaining work and retained failures

The original full canonical 40539 remains a failed historical gate:
496/501 natives and 242/244 E2E labels passed, including one intentional
TLS-stub skip. Its settings and table-lock failures are not silently
deleted, assigned a different SQLSTATE without evidence, or called green.

Ordinary scalar WHERE/ORDER, EXPLAIN scalar-child sort-slot reuse, read-only
transaction/I/O overhead, interval storage input classification, complete
routine handlers and wider query/type/protocol families remain unfinished.
Private interval-parser `4afbdb02` and query-BEGIN status `2f85de1f` are
READY but not integrated during the current source freeze. Their evidence
remains explicitly private; the future new pure interval TU requires a
complete 58-unit ROOT build and fresh signatures after integration.

A separate duplicate-UPDATE diagnostic has actually reproduced a remaining
defect on the exact combined ROOT server: the parser's map overwrites the
first repeated target/RHS. New native 82218 exited 134 (two original sites
became one); new wire 99078 exited 1 with twelve failed assertions, including
accepted invalid assignments and an unintended writer/sequence call. An
actual PostgreSQL 17.2 reference preserves RHS binding/conversion error
priority before duplicate rejection and does not execute that writer.
Artifacts and the uncommitted ordered-site candidate remain in
`/tmp/dbms-update-duplicate-target.2pS2rLEP`; its full fresh 57-source O2
build 5450 is running against the changed AST layout. This is not yet a
verified fix, and legacy UPDATE variants still need complete coverage.

All affected total-checklist families remain partial. The ledger still has
273 items: 22 complete, 166 partial, 70 unverified, 15 deferred_by_user.
The completion gate still rejects outstanding gaps. No Actions enablement,
push, user-deferred security/TDE restart, full TLS runtime, whole-engine
sanitizer or PostgreSQL 18 differential claim is made.
