# RETURNING transition namespaces and typed row channels

INSERT, UPDATE and DELETE now prepare OLD and NEW as RETURNING only
namespaces. Legacy RETURNING expressions preserve canonical identifiers
through execution owned positional bindings. Prepared WITH DML selects its
physical target separately from these logical namespaces and supplies the
actual old and new row images. This closes the reproduced target only
RETURNING defects below, not the complete DML compatibility family.

PostgreSQL exposes old and new values explicitly, normally with a NULL old
row for INSERT and a NULL new row for DELETE. Its UPDATE syntax also allows
transition aliases that hide the corresponding default names.
[PostgreSQL 18 RETURNING](https://www.postgresql.org/docs/18/dml-returning.html),
[PostgreSQL 18 UPDATE](https://www.postgresql.org/docs/18/sql-update.html).
The alias masking, quoted identity and error priority controls here ran
against the actual isolated PostgreSQL 18.6 server with a strict
`server_version_num = 180006` gate. Historical PostgreSQL 17 evidence is not
relabeled as PostgreSQL 18.

## Independent commits

- `6b268130`: RETURNING only logical namespaces, semantic alias collisions
  and default masking, immutable execution copies with typed ordinal cells,
  and ordinary UPDATE target selection excluding logical ranges.
- `ed02ea32`: separately corrects the parser fixture that incorrectly
  rejected `OLD AS new` and `NEW AS old`. Both are positive PostgreSQL 18
  controls; missing AS, trailing commas and duplicate OLD options retain
  their negative grammar assertions.
- `d0cb36ca`: activates those namespaces in prepared WITH DML safely.
  Physical targets require real relation identity. OLD and NEW map once
  from canonical RETURNING options to prepared source occurrences; row
  evaluation reads typed source and column ordinals, not case folded names.
  UPDATE consumes StorageEngine UpdateRowImage, INSERT supplies typed OLD
  NULLs, DELETE supplies typed NEW NULLs, and qualified stars use their own
  prepared descriptors.

The interpreter missing destination guard is an independent prerequisite
`258f41ca`, integrated on the newer production basis as `84e988d2`.

## Preserved failures

The legacy namespace evidence is under
`/tmp/dbms-returning-namespace.UhgHhPMU/`.

- Native `30980`, `returning.native.baseline.log`: the first valid OLD and
  NEW preparation returned `42P01` instead of success.
- Wire `36396`, `returning.wire.baseline.log`: static namespace and alias
  errors differed from the reference, including `42P01` instead of missing
  column `42703`, and parser `42601` instead of duplicate alias `42712`.
- Native `38616`, `returning.native.final.log`: the initial semantic fix
  still exposed the parser duplicate alias error `42601` versus `42712`.
- Expanded baseline `67733`, `returning.wire.baseline.V4.log`: seven real
  assertions failed. Savepoints isolated each case: relation masking gave
  `0A000`, both cross default aliases and quoted aliases gave `42601`.
- Candidate `44266`, `returning.wire.final.V4.log`: `OLD AS o, NEW AS "O"`
  produced `15,15,15`, not the reference `14,15,15`. The legacy RowContext
  normalized the two different canonical aliases to one key.
- Adjacent native `26823` and sanitizer `60467` aborted with structured
  `XX000`, ordinary UPDATE multiple target ranges. The newly registered
  logical ranges exposed a target matcher that checked statement owner
  without physical relation identity. This was not a sanitizer memory
  finding.

The prepared WITH activation evidence is under
`/tmp/dbms-with-returning-transition.3CG5drGX/`.

- Normal optimized native `43960`, `baseline.native.log`: the first WITH
  RETURNING aborted with `XX000`, multiple target occurrences.
- Original unchanged WITH protocol `48076`,
  `baseline.original47.wire.log`: the namespace activation broke existing
  successful RETURNING commands, including the first physical target
  shadowing control. That complete failing run is retained; it was not
  replaced with a smaller or unsupported expectation.

## Legacy namespace verification

The final immutable legacy tree is
`/tmp/dbms-returning-namespace.UhgHhPMU/repo`.

- Focused native `60161`: terminal 0,
  `returning.native.final.V5.log`.
- Ten matching native adjacent tests `33634`: terminal 0,
  `returning.adjacent.native.final.V5.log`. This includes original
  dml_returning, parser_phase1, query_binding, conflict binding, width and
  strict VALUES, SELECT INTO, query host and missing destination controls.
- Expanded protocol `3566`: terminal 0,
  `returning.wire.final.V5.repeat2.log`. Exact values and OIDs cover INSERT
  OLD NULL, DELETE NEW NULL, quoted aliases and compound operands, actual
  relation masking, cross default masking, static errors and no sequence
  effects before binding errors.
- Conflict adjacent wire `45653` and four further adjacent scripts `23472`:
  terminal 0, `returning.adjacent.conflict.wire.log` and
  `returning.adjacent4.wire.log`. The latter covers procedural destination,
  the complete query binder fixture, typed UPDATE and interval input states.
- PostgreSQL 18.6 exact expanded reference: terminal 0,
  `returning.reference18.V5.log`.
- Fresh parser, binder and DML sanitizer objects, matching instrumented
  interpreter guard and stubs: three native tests `99808`, terminal 0,
  `returning.asan.final.V5.log`. Other 53 non main objects are matching
  uninstrumented development objects; leak detection is disabled.

The legacy development carrier uses matching 58 public layout objects;
changed production units compile with shared flags followed by `-O0`, and
native test sources use `-O2`. Its binary SHA256 is
`ea00b438107a1fae301e632b057a83e548a56adb91909d1d9e8b4eae37572bfb`.
The `verified/` source, header and unchanged source audits are terminal 0.
Two final attempts `47436` and `39479` failed before SQL with
ConnectionAbortedError and remain in the V5 startup logs. No timeout or SQL
assertion was changed. Native PID 784365 was observed waiting in
`jbd2_log_wait_commit`; that observation does not establish the cause of
the startup failures.

## Prepared WITH verification

The immutable optimized tree is
`/tmp/dbms-with-returning-transition.3CG5drGX/repo`, based on production
`84e988d2` plus the two namespace commits.

- Fresh changed parser, binder and DML compilation plus focused native
  `15639`: terminal 0, `final.native.log`.
- Eight matching native tests `3854`: terminal 0,
  `final.adjacent.native.log`. Existing WITH bound DML and native command
  view controls retain their original atomicity and fixed snapshot checks.
- Four complete serial wire scripts `19610`: terminal 0,
  `final.wire.log`. Individual logs in `final/` retain the new 19 transition
  controls, unchanged original 47 WITH controls, original four interval
  WITH assertions, and the expanded legacy namespace fixture.
- Exact new PostgreSQL 18.6 transition reference: terminal 0,
  `reference18.log`. This includes BIGINT OID 20, INTEGER OID 23, text NULL
  versus empty text, quoted dot aliases, qualified stars, zero affected
  rows, writing CTE old and new outputs, and no repeated writer effects.
- Fresh parser, binder and DML ASan and UBSan objects and fresh stubs: three
  native tests `8811`, terminal 0, `final.asan.log`. Other 54 non main
  production objects are the matching uninstrumented optimized basis;
  leak detection is disabled.

The basis is the immutable production 58 object archive supplied with
`source-84e988d2.tar`. All 58 stored production signatures were reproduced
using the archived bytes and original logical root paths. All public
headers, the source manifest and the other 55 production sources matched.
Before and after source and header audits, and native, wire, sanitizer and
post commit audits in `final/`, are terminal 0. The final binary SHA256 is
`4d5f03f3176a90210f6e6b053b984d487b420421ed5fb56f43b360c4a1666506`.

## Remaining compatibility work

Prepared WITH UPDATE FROM and DELETE USING still retain their previous
explicit unsupported boundary. Their future source provider must preserve
actual source occurrences, row identity, transition images and the full
visible source expansion of an unqualified RETURNING star. MERGE, broader
RETURNING scalar child and stored function coverage, direct legacy paths
that do not yet invoke whole preparation, and procedural RETURNING INTO
are not closed by these commits. No full database family or canonical
suite approval follows from the scoped tests above.
