# Native/storage SQL pattern consumers

The retained storage matcher used byte `_`, ASCII ILIKE, and no native
SIMILAR operator. `StorageEngine::query`, `queryExpr`, bitmap predicates,
`compareValues`, and native JOIN/DML are real consumers, not an invented
force-legacy switch. They now delegate to `expression/SqlPattern.h`.

`Condition` retains NULL/type/ESCAPE information, including a column ESCAPE.
Decoded native data remains data: text `NULL`, SQL NULL, empty text and empty
BYTEA are distinct. BYTEA LIKE is byte-oriented; BYTEA ILIKE/SIMILAR, scalar
non-text types and arrays reject with 42883. CHAR left padding remains;
CHAR pattern/escape arguments undergo the SQL TEXT conversion. Actual
source collations are carried through typed predicate and JOIN row contexts.
Static pattern type checks do not run functions on empty scans. Demanded
trailing escapes still raise 22025; unused/NULL demand remains unchanged.

The legacy 0x01 escape compatibility decoder applies only to an untyped
compact literal, never a typed column datum. `main.cpp` retains pattern SQL
as a typed predicate and renders parser-owned three-operand ESCAPE grammar
at the existing JOIN binding boundary. It no longer substitutes ECMAScript
REGEXP for SIMILAR or rewrites ESCAPE as a one-byte marker.

## Evidence

Private selected base is c465f8988d3e90209185431dfaf92bef5a4f4f61, not a
later ROOT header epoch. Artifacts are in
`/tmp/dbms-legacy-sql-pattern.j2twXvZX`.

- Earlier exact feba0f76 own all-58 baseline completed 0; native original
  16 failure controls completed 134 (`native-feba-baseline-runtime.log`).
  This is the old feba consumer epoch, not falsely relabeled as c465.
- c465 V1 own fresh 58 completed 0; expanded whole failed on C.utf8 42704
  and the actual legacy JOIN ON pattern 0A000. Uppercase parser ESCAPE and
  UNKNOWN-to-BYTEA native failures are retained in V1/V2 logs.
- New strict PostgreSQL 18.6 wire oracle verifies 180006 and completes all
  62 exact datum/type/NULL/ESCAPE/JOIN/DML controls. Original 13 and 90
  matrices independently complete 0 with the strict reference too.
- Public-header epoch V2 rebuilt all 58 from source; V3 changed three CPP
  units with all other 55 source/header/flag receipts checked. V3 complete
  native and scoped SAN reached one retained failure: raw column 0x01.
- Final V4 sole changed TableManage CPP plus matching other 57 receipts,
  all relative headers, flags and binary stamp complete 0. Frozen SHA256:
  `f943cd1e81da92fb19ef2373f6aa3438215c29bcc9f5388697be33ce06c544c4`.
- Final 14 original/new whole scripts run strictly serial and complete 0
  (`final-whole-v4.log`); all owned server cleanup runs and no frozen V4
  server PID remains. Original 13/90 SQL, assertions and default 15-second
  deadline are unchanged. Original forced-ANALYZE fixture is retained; its
  name does not imply a force-native query switch.
- Final 11 native fixtures complete 0 (`native-group-v4.log`), including
  actual direct compact API, buffered NULL bitmap, five JOIN kinds,
  column ESCAPE, C collation, index companion and native mutation checks.
- Scoped ASan/UBSan instruments TableManage, ExprEvaluator and expr_helper
  production TUs plus the new native test/stubs; other 54 production TUs
  use matching normal objects. It completes 0 (`scoped-san-v4.log`). This
  is not all-58 SAN, and leak detection is disabled.
- Full source/header/normal-object byte hashes, receipt and stamp audit
  completes 0 (`audit-final-v4.log`). All actual failed runs remain.

This closes these tested SQL pattern consumers. It does not claim all
collation families, every PostgreSQL ARE extension or every SQL shape.
The independent negative native DELETE diagnostic found an implicit
transaction owner left active after a predicate DbError, with no row loss
shown. That separate owner-boundary investigation is not part of this fix.
