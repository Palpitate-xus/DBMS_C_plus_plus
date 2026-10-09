# Scalar SQL function queries and unnamed parameters

| Local commit | Repair |
| --- | --- |
| `00675e31` | Keep audit progress out of the evergreen README; audit files remain available. |
| `5ded1ab9` | Bind and execute the whole scalar SQL query body, rather than stripping SELECT and rejecting FROM/WHERE/ORDER. Preserve typed runtime arguments, independent function namespace, column-over-argument precedence, first-row consumption and empty-result NULL. |
| `8b03449a` | Parse unnamed type declarations with the shared type grammar, reject empty comma-separated arguments, and persist an unnamed single argument when its type is present. |

The two source repairs bring the scoped repair count to172. The inventory is
762 automatic native tests +2 actual Main drivers,426 registered protocol/E2E
entries and58 production compilation units. This is not whole-suite completion.

[PostgreSQL's SQL-function documentation](https://www.postgresql.org/docs/18/xfunc-sql.html)
defines scalar functions' first-row/empty-NULL behavior and column precedence
over identically named arguments. The new paths retain actual prepared AST,
source descriptors, parameter origins, rows and NULL bitmaps; they do not
render runtime values into SQL literals or borrow an outer CTE namespace.

## Evidence and verification plan

Artifacts: `/tmp/dbms-root-catalog-followup.PAz6zkPp`.

1. Preserve the previous frozen production binary and reproduce failures.
2. Run the exact new protocol SQL and expected values/OIDs on real PostgreSQL
   18.6, including named Extended Query statements and runtime TEXT/NULL cells.
3. Recompile all58 own-path production units after the public-header changes;
   validate receipts, normal link/cache, repeat build and immutable binary.
4. Run complete adjacent native/protocol fixtures and the unchanged BIT384
   comparison. Keep every original failure; repair independent causes in
   separate commits without changing expected SQL/rows/OIDs/counts/timeouts.
5. Continue recursive CTE argument binding, quoted-range HAVING and earlier
   whole-run failures; do not mark the original requirement families complete.

| Invocation or artifact | Actual result |
| --- | --- |
| `sql-body-old-baseline.log` | Exit1. Original query-function calls22023; unnamed declaration42704 and calls42883. First Extended runtime NULL call also22023. |
| `sql-body-reference18.log` | Exit0 against actual180006:26 statement checks including BEGIN, plus4 Extended executions and exact TEXT descriptor/origin. |
| `sql-body-fresh57.log` / `sql-body-fresh-main.log` | 57 units exit0; first Main attempt exit1 from a wrong session-variable reference, retained. |
| `sql-body-fresh-main-corrected.log` | Exit0; genuine corrected Main compile, giving57+1 current own-path units. |
| `sql-body-native-full.log` | Exit0; all11 original/new complete native fixtures. |
| `sql-body-gates-initial.log` | Exit1;11 native pass,21 protocol entries18 pass/3 fail. Original CTE reader assertions fixed; recursive writer and its missing effects remain. New query-body fixture only3 unnamed-signature assertions fail. Quoted-range HAVING failure retained. All58 receipt/cache/repeat/source/binary fences pass. |
| `sql-body-bit384.log` | Exit1 before comparisons: default reference was170002, correctly rejected by the version guard. |
| `sql-body-bit384-reference18.log` | Exit0 after explicitly selecting180006; unchanged384 cases,zero differences. |
| `sql-body-unnamed-gates.log` | Exit1; parser negative fixture134 plus unnamed single-argument persistence omissions retained, as well as the original CTE/HAVING failures. Actual58 fences pass. |
| Corrected parser/storage builds | Both exit0; only2 changed CPP units rebuilt, other56 verified own-path objects reused. Not a second fresh58 build. |
| `sql-body-final-protocol.log` | Exit0; all25 statement checks plus4 Extended executions pass. Actual runtime NULL, literal `NULL`, empty string and apostrophe preserved. |
| `sql-body-final-bit384-reference18.log` | Exit0; unchanged384 actual PostgreSQL18.6 comparisons,zero differences. |
| `sql-body-final-gates.log` | Exit1; all12 complete native fixtures pass. Complete21 protocol entries19 pass/2 fail: original recursive CTE writer and original quoted-range HAVING. Final58 receipt/cache/repeat/source/frozen fences all pass. |

Final frozen binary: `dbms_main.sql-body-final.frozen`.
SHA256: `821f72fb64ead3cacb8097ecfeb883769794e6996d3926deee1a27e801aa3311`.
Source seal: `06ccca0f45938cb2c5aef841d0a527cc24b98924c0ac42e2e7b76e4430594fbd`.
Both preceding failed frozen generations remain available.

## Remaining scope

The exact original recursive writer query still reports
`cte_owner_writer(unknown)`42883, and its expected three inserted rows are
missing. The original quoted-range GROUP BY/HAVING fixture still returns both
rows instead of one. These are not replaced by weaker fixtures.

This change does not implement all SQL-function statement lists, procedures,
DML-returning bodies, return-type assignment-cast eligibility, or all
aggregate/set/SRF execution graphs. Other earlier whole-run failures remain
open. No new completed762+2/426 full-suite, sanitizer or real-TLS proof.

Original273 statuses remain22 complete/166 partial/70 unverified/15 user-deferred.
The items match the previous `f80f86af` ledger exactly. Their SHA256 using
sorted-key, compact, UTF-8 JSON serialization is
`74923d7574a36d2dcf83e76aa2e86d1c90d6e7a1f27533209377c9266fe795e5`
in both versions; this specifies the serialization rather than reusing an
older checkpoint's differently reported hash.
No push, GitHub Actions activation or user-deferred branch restart.
