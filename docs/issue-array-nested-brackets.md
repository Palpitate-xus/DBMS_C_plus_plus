# Nested ARRAY bracket constructor grammar

PostgreSQL 18.6 accepts `ARRAY[[1,NULL],[2,3]]` as the same `integer[]` datum
as `ARRAY[ARRAY[1,NULL],ARRAY[2,3]]`. The array ALTER value regression exposed
the shorthand failing with SQLSTATE 42601 in the INSERT parser.

The parser now reads nested bracket contents recursively inside an ARRAY
constructor. Bare brackets remain invalid primary expressions, and a failed
primary cannot become an array subscript whose receiver is missing. Missing
closing brackets, trailing commas, and empty element slots remain syntax
errors.

The dedicated native grammar/value test passed with normal O2 production
objects. Strict PostgreSQL 18.6 confirmed both positive spellings and the four
negative grammar controls. The complete array ALTER protocol fixture reached
and passed its shorthand INSERT, dimensions, real conversions, and rollback
checks; its subsequent unique-index SQLSTATE mismatch is a separate issue.

Evidence is retained under `/tmp/dbms-alter-array-values.yC5kFpDd/`:
`wire-v1-full.log` preserves the original shorthand failure,
`parser-v2-native.log` and `parser-v3-native.log` retain the negative grammar
failures, `parser-v4-native.log` passes, and `wire-v4-full.log` preserves the
later independent unique-index failure. Production used all 58 freshly
compiled normal O2 objects after the shared header change, followed by the
changed parser object rebuild; all source/header/flags/object signatures and
the binary stamp were checked. Protocol timeout remained 15 seconds. The V4
protocol semantic data directory used tmpfs; this is not disk performance
evidence.
