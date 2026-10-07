# Reference-aware reuse of unchanged savepoint images

This is separate from the preceding no-effect restore patch. Creating another
savepoint still used to copy and sync another entire physical database image,
even when an existing live savepoint already owned exactly that image.
`savepoint_image_alias_test` reproduced this with the existing engine APIs:
the second unchanged savepoint produced two images. Baseline tool 57147 exited
134; its original log remains at
`/tmp/dbms-savepoint-image-demand.9gzfRDln/alias-baseline.log`.

The consumer now shares an existing image only when its row/DDL/logical log
boundaries match **and** the complete no-effect receipt proves actual current
payload, cached catalog metadata, flushed WAL/CLOG and durable cache state.
Equal counters alone are never sufficient. Savepoint mode, deferred checks,
lock checkpoints and temporary-session state are still captured independently.

A restore no longer consumes a backup referenced by another live frame.
ROLLBACK TO updates its own replacement, removes newer frames, then discards
only images with no remaining references. RELEASE similarly removes frames
before collection. COMMIT and full abort clear all frame references before
discarding each distinct image. There is no hidden retained-image cache,
unbounded ownership registry or new public layout/API. Images after real
unproven changes still take the original complete restore path.

## Matching scope and real controls

The private tree is `/tmp/dbms-savepoint-image-alias.EI8b3LsW/repo`, based on
`1a8336db` and its independent catalog/derived-map dependencies. All 58 donor
sources, all relative production headers and actual flags were compared to
the immutable no-effect donor before copying. Object receipts were then
readdressed to this private path. Exact donor link/repeat audit 43796 exited 0;
the changed TableManage compilation/repeat/all-58 signature check in 94440
exited 0. No public header changed, and this is a matching O0 donor plus fresh
changed-TU proof, not a new optimized/full-58 source compilation.

Two setup helper errors (a nonexistent stamp-helper name and a missing
`dbms_main_sources` call) are not database reds or passing build evidence.
They were corrected before the successful full-source/header/flag audit.

- Native 94440, exit 0: singleton reuse; multiple duplicate names; real row
  writes and repeated older/newer rollbacks; releasing one alias without
  consuming another; commit/abort cleanup; physical DDL PREPARE rejection;
  malformed shared-image fail-closed abort without loss of committed rows.
- Expanded native 12585, exit 0: a raw physical file is added without changing
  any log counters **before** the next savepoint. It correctly gets a distinct
  preimage; rollback to the newer boundary preserves that file, while rollback
  to the older boundary removes it. No assertion was replaced with a counter
  or unsupported-shape expectation.
- Original 13 adjacent native tests, 13458, exit 0: buffer and derived-map
  owner checks, all savepoint namespace/stack/mode/failure controls, original
  clean DDL rollback, sequence-generation rollback, catalog and unchanged
  foreign-key action/rename/restart controls.
- Entire original prepared-transaction native, 26325, exit 0: in-doubt and
  cross-process restart, CLOG/publication faults, mode reset and temporary/DDL
  prepare rejection; no prepared-state assertions were weakened.
- Scoped ASan/UBSan 20555, exit 0: fresh TableManage/BufferPool/FSM/VM and
  test/stubs, matching other 54 O0 objects, both complete no-effect and alias
  native tests. This is not whole-source sanitizer coverage.

Candidate binary SHA256:
`96c5f76e58a673cb13a633cd740e596f87455ef7deec0e3cd784b011b0809b12`.
Helpers and complete logs are under `/tmp/dbms-savepoint-image-alias.EI8b3LsW`.

## Original default-15-second disk matrices

All original SQL, row/type/state/effect/tag assertions, default deadline and
ordinary disk-backed data-directory choices are unchanged.

- Whole Domain/FK six-case script: 70772, exit 0,
  `alias-domain-wire.log`, actual server 3936023 exited 0.
- Whole original typed UNION ALL script with explicitly owned real-server
  tracing: 15143, exit 0, `alias-append-trace.log` plus its `server.strace`;
  `APPEND FAILURES []`, server 3948817/tracer 3948819 both exited 0.
- Whole original UNION ALL, without tracing: 23189, exit 0,
  `alias-append-repeat.log`; all original effects including count 36 retained;
  server 3961085 exited 0.
- Six original scripts/repetitions: 83880, exit 0, `alias-wire-final.log`:
  UNKNOWN repeated three times, whole fromless demand, read-only savepoint and
  DML lock-error. Each matching per-script log has explicit terminal ownership.

The earlier no-effect-only whole Append failure, tool 38879, remains a genuine
writing UPDATE rollback timeout at the original 15 seconds. The initial
pre-catalog original timeout and ROOT's original timeout/misaligned-finally
logs also remain evidence. Successful repetitions do not erase those failures.
The nontraced Append repeat overlapped the six short scripts in separate
owned data directories/ports, and independent peer/compiler I/O was present.
This is disclosed concurrency, not a controlled throughput comparison.

This closes the two identified needless image operations for proven states,
not all disk-performance, storage, catalog-MVCC or savepoint behavior. Real
writing-image restores and every necessary change fsync are retained. Complex
cache states without a complete owner proof remain conservative. No deadline,
assertion, TMPDIR or source query was changed to manufacture a passing result.
