# Genuine literal-left physical comparison ownership

## Status and scope

The independent Main source root is fixed locally. The complete new 878-control
protocol fixture is **still red**: sixteen SQL NULL comparisons belong to the
separate nullable-condition consumer issue. This commit is not a claim that the
whole comparison family or project checklist is complete, and is not imported
into Root/master. The original VARBIT typed-literal parser issue also remains
open (the original 384-case matrix still has sixteen failures).

## Root cause and bounded repair

The single-source compact receiver requires a physical column on the left.
Its AST adapter admitted reverse INTEGER/UNKNOWN comparisons only. Valid
UNKNOWN or grammar BIT literals on the left of actual BIT/VARBIT columns,
and UNKNOWN literals on the left of actual TEXT columns, therefore reached a
different consumer and returned incorrect rows. Boolean branches were not
lowered as a complete physical-source expression either.

`lowerSourceComparison` now admits these finite scalar comparisons using the
actual source occurrence, descriptor, and real LiteralExpr role. It commutes
`<`, `>`, `<=`, `>=` with the correct inverse, preserves equality/inequality,
and quotes the real physical identifier including embedded quotes. It accepts
only scalar, non-array descriptors and the appropriate real input identity;
typed TEXT expressions are not converted into UNKNOWN input. AND/OR is
lowered recursively only when every leaf is admitted; otherwise the entire
original expression remains unchanged. Parentheses retain boolean precedence.
Generated connective spelling is lowercase because that is the existing
compact consumer's actual token contract. No global SQL replacement, row
inspection, routine execution, or parameter-value constant inference is used.

## Complete evidence (private Root104 ABI only)

Artifacts: `/tmp/dbms-bit-reverse-owner.ynoWTxQE`.
Base: `b3622209` (declared-parameter source issue), with frozen original Root104
headers, flags, source manifest and genuine all-58 original compile receipts.
`verify-current.sh` proves all source/header/signature/receipt/cache/object
bytes before using 57 unchanged donor objects and compiling Main normally.
This is **not** a new fresh-58 build or proof for Root107/ParameterOrigin ABI.

| Complete run | Actual result |
| --- | --- |
| owned PostgreSQL 18.6 / version180006, strict fixture | 879 controls, exit0 |
| unchanged b362 baseline, same whole fixture | 878 controls / 166 failures, exit1 |
| final v4 Main candidate, same whole fixture | 878 controls / 16 SQL NULL failures, exit1 |
| original actual Parse/Bind/Describe/Execute probe | 123 controls / 0 failures, exit0 |
| complete fresh-driver native gates | 33 suites / 0 failures, exit0 |
| complete protocol neighbors plus new fixture | 29 suites / 1 failing suite, exit1; all 28 original neighbors passed |

The fixture retains all seven scalar operators, both literal directions,
indexed/nonindexed VARBIT, true BIT grammar literals, UNKNOWN b/B/x/X prefixes,
empty and populated real tables, physical NULLs, actual SQL INTEGER/OID23,
quoted/space/embedded-quote physical columns, real BIT primary keys, TEXT data,
typed TEXT invalid signatures, invalid-input admission at LIMIT0/false/OR,
and genuine stored-writer zero/once effects. Reference-only SET adds the one
strict control; SQL/assertions/timeouts/disk defaults are otherwise identical.

Logs are `bit-reverse-strict18-v1.log`, `bit-reverse-baseline-b362-v1.log`,
`bit-reverse-candidate-v4.log`, `bit-parameter-source-reverse-v4.log`,
`bit-reverse-native-v4.log`, and `bit-reverse-whole-v4.log`.
The v1/v2/v3 actual red logs are retained: v1 left TEXT/OR plus NULL produced
18 failures; v2/v3 retained the uppercase-connective OR error plus 16 NULLs.
None is presented as a successful terminal run.

Final normal log: `bit-reverse-normal-v4.log`; repeat build actually reports
up-to-date after linking. Frozen binary `dbms_main.bit-reverse.frozen` SHA256:
`211ff9aae2a31ace6930b90aa8436ca1ce1b02d86219d0492c83196fcfcf0906`.
`gates-current.sh` holds the exact native/protocol file lists, compiles fresh
native drivers/stubs, checks all current signatures and frozen bytes before
and after, and uses normal disk-backed isolated data directories. No Root,
frozen donor, old fixture, skipped branch, push, or GitHub Actions was changed.
