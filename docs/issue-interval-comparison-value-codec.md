# Shared finite INTERVAL value comparison

The shared comparison helper previously compared interval display strings.
It now parses the existing typed interval datum and compares a signed 128-bit
microsecond span: `(months * 30 + days) * 86400000000 + micros`. This preserves
the PostgreSQL finite interval ordering/equality convention and avoids an
int64 intermediate overflow for valid month/day fields. Input errors retain
the interval parser's structured SQLSTATE.

Ordinary binary comparisons, simple CASE and the new quantified receiver use
the same helper. The receiver's hash key uses that same normalized span; a
separate quantifier-only string special case is not the repair.

This does not close the interval, infinite-interval, custom-type or operator
catalog families. It specifically repairs actual `1 mon = 30 days`,
`1 day = 24 hours` and `1 mon > 29 days` differences. The actual reference
is PostgreSQL 18.6 (`180006`), not relabeled 17.2 results. Its
[interval comparator](https://github.com/postgres/postgres/blob/REL_18_6/src/backend/utils/adt/timestamp.c)
uses the same 30-day/128-bit convention.

The retained `probe-operator.py` and `reference-demand72-18.log` under
`/tmp/dbms-quantified-comparison.oUtteDhP` contain the reference and earlier
candidate differences. The permanent `interval_comparison_value_test.cpp`
also covers a negative month/day pair and simple CASE. Final integration
evidence, exact binary/header provenance and the preserved failures are in
[the quantified execution proof](issue-quantified-prepared-execution.md).
That proof uses the complete new-header candidate, not old-ABI objects claimed
to match this standalone narrow commit.
