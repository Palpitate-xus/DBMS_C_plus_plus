# Open SELECT INTO execution-demand bug

Date: 2026-10-06. Reproduced; implementation and combined verification remain
open. SQL-04/FUNC-06 remain partial. A correct first-row value or P0003 alone
does not prove that subsequent projection expressions were not executed.

## Actual baseline and reference

The diagnostic used the frozen atomicity candidate `3d6f88d3` (integrated as
`5411a7aa`), not a newly built ROOT combination. Its SHA256 starts `3ecec7d5`.
Artifacts are retained in `/tmp/dbms-plpgsql-into-rowcap.ZX279noB`:
`ours-results.json`, `reference.sql`, `reference.stdout`, `reference.stderr`.
One owned isolated candidate server (PID 2430276) exited normally with status 0;
no diagnostic server was left running.

The reference reported server_version_num=170002, PostgreSQL 17.2, not 18.6.
After loading PL/pgSQL, extra_errors/extra_warnings were both none. All 21
diagnostic functions were created successfully. The 18 INTO cases and four
ordinary SELECT controls each used a savepoint; the outer transaction ended
with a successful ROLLBACK. Private sequence increments intentionally survive
savepoint rollback and establish how many function calls actually occurred.
An initial invalid reference attempt queried a PL GUC before LOAD and received
42704; that output is retained separately and is not the reference evidence.

Three input rows have id=1/2/3 and payload=11/22/bad. A VOLATILE counted(id)
increments the private sequence and returns id; another VOLATILE function
divides by id-2 or id-3. The following are observed results, not inferred tests.

| Reached INTO query | PostgreSQL 17.2 | Frozen candidate |
| --- | --- | --- |
| counted(id), no ORDER, non-STRICT | Returns 1; one call | Returns 1; three calls |
| counted(id), ORDER BY id, non-STRICT | Returns 1; one call | Returns 1; three calls |
| Those counted queries, STRICT | P0003; two calls | P0003; three calls |
| counted(id), ORDER BY projected alias | Three calls, including with STRICT | Three calls; this is a passing control |
| Writing CTE inserting three rows, final counted(id), non-STRICT | Side table has three rows; one call | Side table has three rows; three calls |
| Same writing CTE, STRICT | P0003; two calls | P0003; three calls |
| CAST(payload AS INT), no ORDER, non-STRICT | Returns 11 | 22P02 from the third row |
| Same CAST query, STRICT | P0003 | 22P02 from the third row |
| CAST query with ORDER BY id or projected alias | 22P02 | 22P02; these are passing controls |
| VOLATILE division fails on row two, ORDER BY id, non-STRICT | Returns -12 | 22012 |
| Same second-row division, STRICT | 22012 | 22012; a passing control |
| VOLATILE division fails on row three, ORDER BY id, non-STRICT | Returns -6 | 22012 |
| Same third-row division, STRICT | P0003 | 22012 |

Ordinary SELECT counted(id) returns all three rows and makes three calls on
both engines. Ordinary CAST and division SELECTs fail identically. Consequently
the failure is a procedural output-demand contract, not a reason to globally
truncate table scans, suppress errors, or stop all ordinary SELECTs early.

## Matching ROOT combination also remains red

The new full-demand regression was run against ROOT's formal optimized
`5411a7aa` combination, frozen SHA256
`c911593edbec82aaf96e5b67879b74bfe5c0ea955d605166b770f17a019c21f5`.
Terminal 42311 exited 1 after completing all 22 controls and ROLLBACK:
11 differences were retained (six counted-call cases, two unordered CAST,
non-STRICT second-row division and both third-row division cases). ORDER
projected/immutable CAST ORDER and ordinary SELECT controls did not differ.
The runner finally cleaned its owned server.

Exact baseline output is transcribed, explicitly not a second run, in
`/tmp/dbms-plpgsql-into-rowcap.ZX279noB/combination-5411a7aa-baseline-red.txt`.
The pending private implementation's permanent regression source is
`/tmp/dbms-plpgsql-row-demand-fix.UnioEpqx/repo/tests/plpgsql_select_into_execution_demand_protocol_e2e_test.py`.
It is not yet an integrated passing ROOT test. This evidence removes any
assumption that the independent lexical/atomicity combination fixed the demand.

## Cause and required implementation

`PlPgsqlHost::query(string)` carries no execution demand. The server executes
and materializes the complete query before returning rows.size(); only then
does the interpreter choose the first row or reject multiple rows. Native
`plpgsqlQueryNative` has the opposite suspected inconsistency: it reports all
qualifying rows but projects only the first one. That native observation is
currently a source inference, not an executed native reproduction.

PostgreSQL 18's [PL/pgSQL executor source](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/pl/plpgsql/src/pl_exec.c)
sets tcount to one for ordinary INTO and two for STRICT/modifying RETURNING,
with zero meaning unbounded execution. The [basic-statement contract](https://www.postgresql.org/docs/18/plpgsql-statements.html)
also rejects multiple modifying RETURNING rows without explicit STRICT. The
[SPI execute contract](https://www.postgresql.org/docs/18/spi-spi-execute.html)
distinguishes bounded top-level RETURNING execution from commands without output.

The fix must pass explicit output demand through the host and execution receiver,
after complete name/type preparation. It cannot append SQL LIMIT, slice a fully
executed result, or prematurely cap scans required for WHERE, sorting, grouping,
DISTINCT or windows. Sort-key expressions still execute as necessary, while
non-key VOLATILE projections must execute only for requested output rows.
Auxiliary writing CTEs complete their writes even when the final SELECT demands
one/two rows. Top-level DML RETURNING has its own bounded command semantics;
PERFORM, non-RETURNING commands and ordinary SELECT remain unbounded.

The implementation needs native and real-wire red-to-green tests, retained
sequence-side-effect controls, error precedence, original SQL LIMIT/OFFSET,
CTE/order variants and neighboring atomicity/typed-INTO tests. No completion,
full-suite or PostgreSQL 18.6 differential is claimed by this diagnostic.
No push, Actions enablement or user-deferred security/TDE work.
