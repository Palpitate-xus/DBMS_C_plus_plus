# Native source-DML name and LEFT source contracts

The original native dml_semantics fixture expected successful unqualified
`RETURNING id, val` while a source also exposes id. The source UPDATE consumer
correctly exposes those statements as 42702. Its first original full adjacent
native group really exits 1 with this fixture exiting 134. That failure and the
old matching fixture's success are retained, not treated as a runtime regression
or hidden by shortening the fixture.

This independent test correction keeps every ambiguous original UPDATE and
DELETE string as a 42702 negative, checking exact unchanged rows with NULL bits,
transaction ownership and no published successful result. Qualified positive
statements follow, preserving every original successful row/count expectation.
The exact UPDATE strings (including repeat invocation, INNER JOIN and duplicate
source) and DELETE strings (simple, joined, duplicate source) were replayed on
strictly checked local PostgreSQL 18.6, with no-effect and qualified-positive
controls. The reference setup uses session-local temporary tables with the same
column schemas to isolate the unqualified original SQL; it does not claim that
a C++ native fixture itself is PostgreSQL SQL or touch another owner's tables.

The original legal LEFT UPDATE succeeds in PostgreSQL and yields exact values
100/200/300. The strengthened native control retains that SQL, checks all values
and false NULL bits, and then explicitly restores its prior 110/220/330 stage
image so every original later DELETE value assertion remains unchanged. The
original LEFT DELETE with an excluding predicate is checked as successful
DELETE 0, not incorrectly as a PostgreSQL unsupported requirement. The old
unsupported behavior remains in historical logs.

The native DELETE USING adapter is still a distinct runtime gap at the time of
this oracle correction. These new negative and LEFT controls are deliberately
allowed to fail on that old consumer; changing the test is not a claim that its
runtime is complete. The subsequent source carrier and independent native
DELETE USING runtime changes must reach a real full-fixture terminal before
the combined set is recommended for integration. Cursors retain their existing
unsupported boundary and are not claimed PostgreSQL-complete here.

Evidence directory `/tmp/dbms-update-from-carrier.Pjk5Ul4Z` retains the original
`native33-update-source-v1.log`, exact original/extended strict oracle logs,
`native-dml-semantics-corrected-oracle-v1.log`, and all baseline/setup/ordering
failure logs. No production source belongs to this oracle correction.
