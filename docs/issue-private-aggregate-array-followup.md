# Prepared aggregate and ALTER array follow-up

README remains evergreen and independently committed as `cfcab3cd`.
This progress evidence belongs here, not in README.

Two independent local commits exist in the private worktree
`/tmp/dbms-having-grammar.MPk64yc0/repo`, branch
`fix/having-grammar-MPk64yc0`. Neither is imported into master while the
preceding master full driver 84918 is running with source inputs frozen.

| Private commit | Result and remaining scope |
| --- | --- |
| `409507fb` | Typed global aggregate lowering repairs the original scalar COUNT child error. Entire unchanged derived fixture passes (90388). All 47 native tests pass; 40 of 41 protocol entries pass (50186 exits 1). The new strict entry retains five failures: four numeric display-scale cases and ROW construction. |
| `4c5b1cdc` | Shared ALTER declaration validation retains complete array envelopes instead of collapsing rank/bounds through a boolean descriptor. Original baseline exits 134 (42973); all 8 native and 8 protocol entries pass (55834/52764 exit 0). |

Aggregate evidence, implementation limits and failed generations are in the
private `docs/issue-prepared-global-aggregate.md`; array evidence is in its
`docs/issue-alter-array-envelope-retention.md`. Logs and frozen binaries remain
under `/tmp/dbms-having-grammar.MPk64yc0`.

Both focused gates verify 58 current own object receipts, cache signatures,
no-recompile repeats, source seals and frozen binary equality at terminal.
Original BIT differential 384 cases passes against verified PostgreSQL 18.6,
server 180006, for the aggregate generation. These are private focused proofs,
not a whole-suite pass or proof for the master generation.

Master remains at 182 scoped source repairs. Its complete driver currently
includes 768 automatic native fixtures plus two Main tests and 432 registered
protocol entries. Its original ALTER array-envelope failure is retained;
the private repair will be integrated only after that driver reaches terminal.

Next work is actual NUMERIC declaration persistence and assignment coercion:
the decimal column factory discards precision/scale, and native writers only
validate an unbounded number. Existing exact arithmetic already preserves
input display scale. Padding aggregate output would not repair column storage.
ROW remains a separate constructor/datum dependency. Reference probes retain
unknown-field comparison errors and three-valued row-expression behavior.

The original 273 item statuses remain 22 complete, 166 partial, 70 unverified
and 15 deferred by user. No push, Actions activation, deadline changes,
deleted failed tests or restart of user-skipped investigations.
