# Original full canonical fd/23a gate: actual terminal failure

Original unchanged `scripts/build_tests.sh` handle23633 actually exits1 on
2026-10-06. All540 native fixtures complete:537 passed/3 failed. All275
registered Python entry points complete:274 PASSED labels/1 failed.
The TLS timeout entry intentionally skips because this binary lacks OpenSSL;
its PASSED label is not TLS runtime evidence. There is no full-green claim.

Immutable runner snapshot is b353fe8b (fd183ec3 source) in
`/tmp/dbms-canonical-input-cursor.4U3gkfUf/repo`.
Frozen original server SHA256 is
`23a0f0c631a1aa837e02396726a426e0f4faf628e6a2a89b1714af3b4eff0acc`.
Actual complete log is
`/tmp/dbms-canonical-input-cursor.4U3gkfUf/full-registered.log`;
last line records Some tests failed. ROOT retained the original immutable
source/test/registry snapshot while later master changed independently.

| Original failure | Actual result and independent follow-up |
| --- | --- |
| matview_test | Original unpopulated source55000 requirement not observed. Later same original control on matching normal4cf returns42P01 for reporting sink because native preparation uses the wrong ambient session. ROOTc38ad83c scopes passed Session and preserves caller restoration; private7 matching native/6 wire/strict18 references actually pass, not a rerun of this whole540 gate. |
| missing_btree_file_guard_test | Original bitmap test only accepts automatic throw although checked executor returns preservedXX001 failure. ROOT8ac621c8 adds explicit failure/exception/no-partial-result checks and throwIfFailed, retaining storage/lock/reindex assertions. |
| missing_memory_index_guard_test | Same checked-result receiver mismatch for Hash/Bloom AND/OR. ROOT284b4e5c retains exactXX001/no implicit file recreation/no heap effects/locks/reindex. Both corrected native fixtures pass on matching normal4cf. |
| postgres_protocol_test.py | Original quantified EXPLAIN line2433 returns no rows. This is still a genuine execution gap. Later stronger whole-binding exposes the distinct invalid ON CONFLICT WHERE fixture at2332; ROOTe5d79650 keeps that exact42702 negative plus qualified positive, after which private full protocol still fails Quant at2444. No original full protocol pass is claimed. |

The full original run actually passes the registered lock timeout/cancellation,
cold recovery, stored-function atomicity and other entries not listed as
failures; that evidence remains tied to fd/23a, not to future master code.
Latest master07803aef has553 native fixtures/288 registered entries and a
changed public CASE layout. Its fresh all58 normal build73985 is still live;
matching69 native/34 wire gates have not started. Source/header/test/registry
freeze is active. Original C-profile and matched en_US full465 differential
timeouts remain separate terminal failures, not hidden by this gate.

Audit273 remains22 complete/166 partial/70 unverified/15 user-deferred.
No full family closure, push, active Actions or resumed security/TDE work.
