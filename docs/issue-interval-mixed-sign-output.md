# Interval mixed-sign output

The shared finite-interval formatter now emits an explicit plus when a positive component follows the last nonzero negative calendar component. A following positive calendar component resets that state, and omitted zero components do not reset it. This follows PostgreSQL 18.6 `datetime.c`'s `AddPostgresIntPart` and `EncodeInterval` postgres-style branches, not a whole-value sign heuristic.

This is a separate representation error from unary interval negation. For example, the stored value `1 month -2 days 3 hours` previously displayed `1 mon -2 days 03:00:00`; the strict 18.6 reference displays `1 mon -2 days +03:00:00`. Calendar/month/day/microsecond values themselves are unchanged, including interval input validation and range errors.

## Actual evidence

The artifacts are in `/tmp/dbms-interval-format-sign.zeWo8MJ6`.

| Evidence | Result |
| --- | --- |
| `native.baseline.log` | Five actual formatter mismatches and the same five stored-value mismatches; exit 134 |
| `baseline.log` | Original mixed-sign SELECT rows mismatch; exit 1 |
| `reference18.log` | Strict official PostgreSQL 18.6, version 180006; complete nine-row matrix passes |
| `build.candidate.log` | Six native controls pass, including shared input/storage/assignment/arithmetic/comparison fixtures |
| `candidate.log` | Complete nine-row wire matrix passes, exit 0 |
| `asan.log` | Formatter/storage native test passes under scoped ASan+UBSan, exit 0 |

The formatter and stubs are freshly optimized. All public header bytes and the other 57 translation-unit source files match the immutable full-58 private donor; the changed formatter source is separately hashed. The source and header audits pass. No ROOT cache or different header-layout objects are reused. Candidate SHA256: `4630b31c0e2c5f72ef243289d927ab8d4e9458e75a0c432421294ae657009f3d`. Sanitizers instrument the changed formatter, stubs and test driver; the other matching translation units are uninstrumented and leak detection is disabled.

Reference setup uses BEGIN, a unique schema, and final ROLLBACK. This patch neither implements other IntervalStyle renderers nor closes finite interval unary arithmetic, interval infinity or the full temporal/type family.
