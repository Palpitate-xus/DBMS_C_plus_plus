# UPDATE FROM / DELETE USING comma-source grammar

The structured DML parser previously retained one FROM/USING item only. A
second comma-separated source caused `42601`, including the original WITH
late-cast rollback control whose PostgreSQL 18.6 result is `22P02`.

`parseDmlSourceList` now represents each comma as a genuine CROSS node. An
explicit JOIN on its right stays inside that subtree, preserving SQL join
precedence and the ON clause's namespace. The existing required ON/USING
validation still rejects malformed joins and trailing commas. Neither SQL
text rewriting nor target-name guessing supplies these occurrences.

Evidence is retained under `/tmp/dbms-with-source-runtime.zl5yD3MZ`:

- The unchanged expanded 47-control protocol matrix passes strict real
  PostgreSQL `180006` (`reference-v2.log`). Frozen ROOT fd183ec3 / binary
  `23a0f0c6…` still returns `42601` for the original comma-source late-cast
  statement (`baseline-v2.log`, terminal 1).
- The new native parser test linked against the preceding matching parser
  object fails its first valid source-list assertion (session 3638,
  terminal 134; `parser-native-baseline-assertion.log`). After rebuilding the
  parser it passes (session 77852, terminal 0; `native-v2.log`). The test
  retains UPDATE and DELETE, comma/JOIN precedence, malformed joins/trailing
  commas and the original WITH envelope.
- The initial private runtime has all 58 objects freshly built under its
  new public headers; changed parser/main objects were rebuilt afterward,
  with header/source audits passing. This is not a claim that the pending
  multi-source mutation runtime or the entire WITH/CTE family is complete.

This commit changes grammar only. Actual multi-source row execution,
nullable source provenance, mutation ownership and output semantics have
separate implementation and verification work.
