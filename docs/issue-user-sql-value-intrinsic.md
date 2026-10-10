# USER is grammar-owned CURRENT_USER identity, not a column or routine

The unchanged original mixed fromless query fails42703 at bare USER after
the genuine pg_typeof repair. The actual original binary44074863 fails four
of the first38 expanded keyword controls, including USER() returning42883
instead of the required syntax42601. The reference is genuine PostgreSQL18.6,
verified server180006; all final41 keyword controls pass on that reference.

Admit bare case-insensitive USER in the parser's existing SQL-value grammar,
retain its `user` spelling/default label and the existing CurrentUser kind.
The shared validator recognizes that real grammar alias, not an added callable
registry entry. Quoted identifiers/routines retain their ordinary namespaces;
USER() remains syntax42601. Existing values, clock/precision, context,
metadata and invalid-call controls are not removed. Native coverage also
proves a registered fake user callee cannot replace the bare keyword, and
protocol coverage retains the NAME OID in an actual stored SQL USER body.

The inline header changed, so all57 own production non-Main units and Main
were genuinely rebuilt (48376/36374 both0,57+1 real compiler entries). No old
header-generation objects or foreign worktree objects/binaries are borrowed.
Matching publication81549 actually exits1: all13native and14/15wire entries
pass, including whole keyword41, pg_typeof56, parameter18, TYPE/catalog/PL
neighbors, original integer350/BIT250 controls and complete default protocol.
All58 own-object/repeat/cache/source/frozen start+terminal checks0. Source seal
`a1aba118bd60f4a703aa7253dbeb5fb72629dba68084d7b173173bfabadd112e`.

The unchanged fromless fixture now reaches the next distinct missing version()
callee42883. It is not weakened, skipped or called complete. Quoted enum
42704 also remains independently reproduced in the current Root generation.
Original273 statuses and15 user-deferred security/TDE requirements unchanged;
no push or Actions activation.

Artifacts: `/tmp/dbms-having-grammar.MPk64yc0`.
Baseline: `user-intrinsic-baseline440.log`.
Reference: `user-intrinsic-stored-reference18.log`.
Builds: `user-intrinsic-own57-build.log`, `user-intrinsic-own-main-build.log`.
Current publication: `user-intrinsic-publication13-15.log`.
