# Clean FSM/VM flush after a cooperating process publication

This is the third independent derived-map problem, after peer pending-cell
coherence (`6f62ac73865049183c3cb332c2069f0bfc5072d5`) and failed-close retry
(`726eb7e14b16635a1a04ebfc590131f2a9238179`). It does not close all storage,
cross-process heap/catalog concurrency, or the original disk deadline issues.

## Actual baseline

An old clean map has not read since another process successfully published new
map bytes. The strict generic `flushChecked()` rejects its old cached bytes.
Consequently `StorageEngine::physicalBackup()` falsely reports failure even
though the child used the same checked map APIs and actual heap-page metadata.

The permanent engine fixture seeds one committed row, a conservative FSM
underestimate, and a legitimately all-visible committed page. A fresh exec
reads the real `PageWrapper` free percentage and publishes it and a conservative
VM false bit. The parent performs **no intervening map read** before backup.

Evidence under `/tmp/dbms-derived-map-clean-peer.j3dcUZ7J`:

| Probe | Actual terminal result |
| --- | --- |
| `baseline.log`, 20445, exact old first-cause object/header/flag audit | exit 134, `backup_after_valid_actual_page_peer=0` |
| `/tmp/dbms-derived-map-coherence.kTZahH6R/clean-exec-flush-probe.log`, 68729 | exit 134, clean FSM and VM flush both false |
| `native-final.log`, first engine fixture in 45628 | success, backup result 1; raw VM, restore and rename assertions also passed |
| `engine-sidecar.log`, 91149 | exit 0, forced unsupported xattrs; genuine engine backup/restore/rename/DROP controls |

The original raw-edit `derived_map_snapshot_test` assertions are unchanged.
Generic clean flush and `quiescentForSnapshot()` remain strict; they are not
silently changed to reload any file with a matching path or timestamp.

## Production contract

Map publication emits a `DMP1` **cooperative candidate**, containing kind,
actual source device/inode, complete byte length, mtime, and SHA-256 of the
complete map bytes. The preferred representation is the inode's
`user.dbms.derived_map_publication` xattr. Unsupported xattrs use a checked
atomic per-map sidecar: temporary write, file sync and checked close, rename,
then directory sync. Xattrs supply whole-value inode-attached metadata;
this is ordinary publication metadata, not authentication or a security
boundary. [Linux xattr documentation](https://man7.org/linux/man-pages/man7/xattr.7.html).

A visible candidate is **not** proof that the producer's following source
fsync succeeded. Explicit engine reconciliation locks the actual source,
checks complete bytes and candidate, independently syncs the source (and the
sidecar/directory when applicable), then rereads and verifies stable source
identity/generation, complete bytes and receipt before changing clean cache
state. Failed sync is failure. File sync and directory-entry sync are distinct
durability requirements. [Linux fsync documentation](https://man7.org/linux/man-pages/man2/fsync.2.html).

The engine calls this explicit reconciliation only after strict map flush
fails. A dirty map cannot discard pending cells through this route. Changed
external bytes without a matching complete cooperative candidate are rejected
by normal refresh and by pending-cell publication too: a setter must not
stamp an arbitrary raw VM=true bit into a supposedly valid publication.

An interrupted local write is separately retryable. The cache may retain an
accepted **physical base**, derived only from the prior acknowledged base
plus successful local write ranges/truncate and an exact reread of those
expected bytes. That digest is not a durable acknowledgement: pending masks
and dirty state remain until checked full publication succeeds. Last-owner
destruction still uses the separately repaired pending FD/cache ownership.

Same-directory fallback hardlink aliases find only exact inode/content
receipts, not matching alias names. Rename/replacement remains actual-FD/path
identity checked. Sidecars and exact `.tmp.<pid>.<ordinal>` artifacts join the
ordinary relation-file rename/DROP/snapshot grouping; the next exclusive
publisher cleans only its own filename's interrupted regular temporaries.

## Verified scopes and retained failures

Fresh candidate compilation used the repository's shared flags followed by
`-O0`. `build-all58.log`, 52878, compiled all 58 production sources, linked,
repeated up-to-date, and verified every object receipt and the binary stamp:
exit 0. Candidate binary SHA-256:
`9b1b86bd163d6426d4bf45ab0785263388416bb3d178be0507823129a2276d93`.
This is a matching development build, not a claim of ROOT's final optimized
combined verification.

The final standalone GNU syscall-fault fixture `publication-fault-final.log`,
84730, exited 0. It covers partial data, stamp, receipt write/sync/rename,
directory sync and source sync failures; each same-object and last-owner
destructor retry; abrupt process exit during receipt write/rename and xattr
stamp; checked consumer failure before later successful independent sync;
same-directory hardlink, copied source without xattr, rename, exact temporary
GC, and strict arbitrary raw VM=true rejection. Failed producers use `_exit`
so destructors cannot manufacture success. Original fsync/pwrite fault
fixtures `snapshot-fault.log` and `close-retry-fault.log` both exited 0.

`publication-sanitized-final.log`, 90746, exited 0 with ASan/UBSan over the two
map implementation CPP files and the final GNU fault test. Leak detection is
disabled for the intentional process-owned pending registry. This is scoped
sanitizer coverage, not all 58 sources sanitized.

`native-final.log`, 45628, passed nine exact natives before a harness filename
typo requested nonexistent `shared_heap_owner_test.cpp`; that compile failure
is retained. `native-storage.log`, 97364, then passed the ten correct unchanged
targets: three `shared_heap_engine_*` owner/rows/index tests, FK action and
rename/reopen recovery, vacuum TOAST, failed savepoint undo, and four physical
backup/restore/manifest tests. `native-publication-final.log`, 97649, reran the
final normal publication test successfully. Thus 19 distinct matching native
fixtures passed, plus the forced-fallback and GNU fault variants.

Original disk/default-15-second wire run 83526 retained all scripts and SQL:
readonly and lock-error/savepoint passed; DomainFK timed out at composite
`CREATE TEMP TABLE` after its first four cases passed; writing Append timed
out at a later `SAVEPOINT app_case` after earlier writer/count/cardinality
assertions passed. `domain-fk-wire.log` and `append-wire.log` preserve the
failures, including the delayed completion consumed by DomainFK's finally.
No timeout, assertions, source SQL, or TMPDIR was relaxed. Parent/peer builds
and independent tests were active, so these observations do not establish a
new causal performance regression or its resolution.

An independent unchanged disk/default-15 repeat, 86881, then finished exit 0
for both complete original DomainFK and writing Append scripts:
`domain-fk-wire-repeat.log` ends `DOMAIN_FK_FAILURES []`, and
`append-wire-repeat.log` ends `APPEND FAILURES []` with the original counter
36. This is additional scoped evidence; it does not erase the two actual
timeouts or justify claiming a general latency cure. Every owned test server
was closed by the original runner's finally path.

## Compatibility boundaries

- FSM/VM byte formats are unchanged. Legacy cold files still open normally;
  there is no automatic data migration just to produce this proof.
- Backup tools may lose xattrs or copy an old sidecar to a new inode. The
  restored file is a new owner; stale existing owners fail closed, and cold
  map opens retain legacy-format compatibility. New checked writes generate
  candidates for the actual restored owner.
- Without inode-attached xattrs, an arbitrary **cross-directory** hardlink
  cannot discover receipts in other unknown directories. The permanent GNU
  control intentionally verifies conservative failure, not fabricated
  coherence. Same-directory fallback and normal engine rename/restore/DROP
  are actually exercised.
- Publication candidates show cooperating map API use, not malicious-tamper
  resistance or a complete independent heap/MVCC reconstruction. No TDE or
  security-specialist work is included.
- Original disk timeouts, broader cross-process storage isolation and all
  unverified crash/fsync or file-lifetime shapes remain visible/open.
