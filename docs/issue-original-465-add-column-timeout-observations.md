# Original 465-case ADD COLUMN timeout observations

## Scope and exact revisions

This is evidence, not a performance fix or full compatibility pass. The original
`fd183ec3`/`b353` full strict PostgreSQL 18.6 runs remain failed: the C-profile run
completed 345 cases before the SERIAL NULL ADD timeout; the en_US run completed
39 before the two-column ADD timeout. Both kept the original 15-second socket
deadline.

The new diagnostic used immutable normal-O2 production `fa193704` binary
`f7ad6b4a39fbd7dd6c1974f27e2c8fb756b511bd1175569a7fa0f0c773ff8ffa`.
It is **not** a matching binary for the diagnostic worktree's `8907d061` source
or the later quantified/source-context integration. `tests/compat` and
`tests/postgres_protocol_test.py` were audited unchanged between these two
source revisions. No SQL, expected result, row descriptor, command tag, deadline,
reference-cluster locale, or global GUC was changed. TMPDIR was `/tmp` (disk).

The reference was the isolated XML-enabled PostgreSQL **180006** at port 15487.
The full repeat used the existing owned `dbms_oracle_en_us_20261006` database:
collation/ctype `en_US.utf8`, libc provider. The older C-profile evidence was not
relabelled as an en_US or later-source run.

## Two exact original cases

Both complete cases matched the reference in fresh isolated C and en_US runs,
with every original SQL/OID/tag/result comparison intact:

| Original statement | C duration | en_US duration | Result |
| --- | ---: | ---: | --- |
| `ALTER TABLE diff_serial_null_alter ADD COLUMN id SERIAL NULL` | 0.075905 s | 0.080938 s | exact 42601 |
| `ALTER TABLE diff_check_add ADD COLUMN left_value INT, ADD COLUMN right_value INT` | 0.682561 s | 0.861203 s | ALTER TABLE success; subsequent four-column rows/NULLs matched |

Artifacts: `/tmp/dbms-add-column-timeout.i8HonnoR/original-f7-c.log` and
`original-f7-en-us.log`; tool sessions 25783 and 58236 both actually terminated
with exit 0. These later isolated passes do not establish the cause or closure
of either older full-state timeout.

## One authorized full repeat: actual terminal failure

The full 465-case en_US repeat (session 32767) **terminated with exit 1**.
Its immutable inputs and original 15-second deadline were preserved.

- 190 cases completed: 188 matched, two genuine old-f7 quantified differences
  (`any_all_null`, `fromless_where`). Those differences are not evidence against
  a later matching quantified candidate.
- Case 191, `insert_select_null_bitmap`, failed at statement 21/21:
  `DROP TABLE diff_insert_null_target,diff_insert_null_star,diff_insert_null_projected,diff_insert_nonnull_target,diff_insert_null_source`.
  The local socket timed out after **15.004197 seconds**.
- The preceding 20 statements of that incomplete case received responses, but
  the runner compares a case only after its final statement. They are not
  counted as a completed matching case.
- 2,080 SQL responses completed; 274 later cases/3,068 later SQL statements
  never started. Total uncompleted cases: 275 (the incomplete current case plus
  those 274).
- Original `check_add_validation` (case 40) fully matched even in this accumulated
  full state. Its two-column ADD took 2.049723 seconds. The SERIAL NULL case
  (case 346) was never reached.

Full artifact: `/tmp/dbms-add-column-timeout.i8HonnoR/full-original465-f7-en-us.log`.
The owned server was stopped by the runner's final cleanup and its PID was gone.
No restart or raised deadline converted this failed run into a pass.

## Observations, not established causes

During the failed DROP, its worker was observed in `submit_bio_wait` at about
3.0 and 9.738 seconds, running at 6.37 seconds, and in `jbd2_log_wait_commit` at
13.093 seconds. These are phase-specific kernel-wait observations, not proof
that either older ADD timeout had the same cause.

Earlier in the full run, `/proc` reported about 2.805 GB process write bytes and
94,879 write syscalls while the live data directory contained about 7.153 MB.
This is observed write amplification, **not** an fsync count or a demonstrated
fix opportunity safe to apply without transaction/DDL rollback auditing.
Global iostat samples are not attributed exclusively to this server.

`server-full-f7-en-us.log` also contains repeated CTE temporary-heap allocator
flush failures followed by cached-writeback discard (`__tmp_*___cte_0.dt`).
Those messages require a separate lifecycle/cache-ownership investigation;
they do not establish the cause of the DROP or ADD timeout. Likewise, early
physical DDL backups before pure declaration validation are a separate,
currently read-only audit. Neither safeguard is disabled by this report.

All raw logs, query-stage timing, input manifests, server output, and observer
receipts are retained in `/tmp/dbms-add-column-timeout.i8HonnoR`. Full-pass,
performance-causality, durability, and database-family closure claims remain
explicitly unmade.
