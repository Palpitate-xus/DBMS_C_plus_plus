# Prepared aggregate and ALTER array follow-up

README remains evergreen and independently committed as `cfcab3cd`.
This progress evidence belongs here, not in README.

Three independent local commits exist in the private worktree
`/tmp/dbms-having-grammar.MPk64yc0/repo`, branch
`fix/having-grammar-MPk64yc0`. Neither is imported into master while the
preceding master full driver 84918 is running with source inputs frozen.

| Private commit | Result and remaining scope |
| --- | --- |
| `409507fb` | Typed global aggregate lowering repairs the original scalar COUNT child error. Entire unchanged derived fixture passes (90388). All 47 native tests pass; 40 of 41 protocol entries pass (50186 exits 1). The new strict entry retains five failures: four numeric display-scale cases and ROW construction. |
| `4c5b1cdc` | Shared ALTER declaration validation retains complete array envelopes instead of collapsing rank/bounds through a boolean descriptor. Original baseline exits 134 (42973); all 8 native and 8 protocol entries pass (55834/52764 exit 0). |
| `2144d7d7` | NUMERIC column declaration modifiers persist and govern scalar/array assignment, rounding, overflow, ALTER rewriting and wire metadata. Reference 26 controls passes; baseline 22 controls has 12 failures. Corrected 78431 exits 1 with all 13 native and 6/7 wire entries passing, including all 25 numeric controls and the entire default protocol. Only the independent strict ROW control remains failed. |

Aggregate evidence, implementation limits and failed generations are in the
private `docs/issue-prepared-global-aggregate.md`; array evidence is in its
`docs/issue-alter-array-envelope-retention.md`. Logs and frozen binaries remain
under `/tmp/dbms-having-grammar.MPk64yc0`.
Numeric proof and all failed iterations are in private
`docs/issue-numeric-column-modifier.md`. Its corrected BIT384 probe 59453 exits
0 against actual 180006. All 58 current own receipts/cache/repeat/source/frozen
terminal fences pass. Frozen SHA d639c6bf83ea324c3b785ee29e98826448b3b6c44ce36bf8f2760a82e4405f98,
source seal bd4f2e198acfd8670d7894c7fa9c8a83cbce25736f4aa53871b2ecd3bb82827d.

Both focused gates verify 58 current own object receipts, cache signatures,
no-recompile repeats, source seals and frozen binary equality at terminal.
Original BIT differential 384 cases passes against verified PostgreSQL 18.6,
server 180006, for the aggregate generation. These are private focused proofs,
not a whole-suite pass or proof for the master generation.

Master remains at 182 scoped source repairs. Its complete driver currently
includes 768 automatic native fixtures plus two Main tests and 432 registered
protocol entries. Its original ALTER array-envelope failure is retained;
the private repair will be integrated only after that driver reaches terminal.

The four numeric aggregate display-scale failures are repaired through actual
column coercion, not padded aggregate output. The legacy native factory API
still does not itself attach precision/scale; that independent contract remains
open and is explicitly documented. Numeric physical capacity and the entire
type family are not declared complete. ROW is the next separate constructor/
datum dependency. Reference probes retain unknown-field comparison errors and
three-valued row-expression behavior. Private now has 185 scoped repairs,
770 automatic native fixtures plus two Main tests and 434 registered protocol
entries; none of these three new source commits is imported into master yet.

The original 273 item statuses remain 22 complete, 166 partial, 70 unverified
and 15 deferred by user. No push, Actions activation, deadline changes,
deleted failed tests or restart of user-skipped investigations.
