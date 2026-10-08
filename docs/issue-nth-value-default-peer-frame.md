# NTH_VALUE uses the entire unordered default peer frame

Independent repair following frozen admission prefix `0e6a34e3`.
Main's legacy NTH_VALUE producer initialized its frame end to the current
row even when ORDER BY was absent. The default RANGE frame includes all
peers; without an order key, the entire partition is one peer group. Thus
NTH_VALUE(v,2) returned NULL on the first row even when its partition had
three non-NULL values. PARTITION-only reproduces this on original Root108,
independently of the newer plain-OVER admission repair.

Only the genuine existing default-frame end initialization changes. It uses
the already computed actual partition end if there is neither an explicit
frame nor an order key. Explicit ROWS bounds and ordered peer extension keep
their existing current-row initializer. No ORDER key, row reordering,
argument/value coercion, parser/public-header or result descriptor changes.

## Strong complete proof

The permanent80 protocol matrix executes all four positive NTH positions
against populated, all-NULL, empty, WHERE FALSE and LIMIT0 inputs. Four window
specifications cover plain OVER, PARTITION-only, ordered default RANGE and
explicit ROWS prefix. Genuine NULL, rows/headers/OID23/tag are independently
checked. No-order results use identical input values and exact multisets;
ordered controls additionally check each ID's exact expected value. No
unspecified heap ordering is required for the ROWS/no-order controls.

Artifacts: `/tmp/dbms-root-unordered-window.C4g0LIT2`.

| Actual complete input | Actual terminal |
| --- | --- |
| Owned strict PostgreSQL18.6/180006, all80 | 0, all assertions pass |
| Original Root1080d, same80 | 57891=1, all80 finish,10 failures including two already-admitted PARTITION-only frame cases |
| Frozen admission-only00b prefix, same80 | 39446=1, all80 finish, exactly4 no-order default-frame cases |
| Normal sole fresh Main build/repeat, all58 independent receipts/cache/freeze | 40826=0, actual1 fresh CPP plus57 unchanged current proved normal objects, not fresh58 |
| All7 full composed protocol drivers, default disk/deadlines | 95717=0; original84 and new80 both pass, all five old window/descriptor wrappers pass |
| All5 fresh complete native drivers/stubs, default disk | 35060=0; original window/NULL144/GROUPS72/metadata/Volcano controls retained |
| Separately registered genuine current Main frontend58 | 24206=0; no stubs, actual isolated data directory, before/after complete source/test/receipt/frozen-input checks |

Frozen composed binary SHA256:
`c7abd647dad7e633de9f4e09f387640f100cbdae121ac8afdd1fb03c3eec568b`.
The earlier admission-only full84/6 whole failure, original10/4 frame failures,
frozen source epoch and old binary/object/receipt/input seals are retained.
The complete old84 fixture and every preceding full neighbour remain intact.

## Boundaries still open

This finite end-bound repair is not generic NTH_VALUE evaluation: arbitrary
argument expressions, dynamic positions, invalid position values, qualified
physical names, inherited/named frames, leading explicit-frame grammar,
general start-bound/exclusion combinations and actual stored callee ownership
still need their own producer/grammar proofs. No whole-window family, QRY-08
or original273 requirement is promoted to complete by this scope.

INTEGER full88 remains held, original CTE/scalar/type/enum/catalog/storage/
recovery/operations requirements remain open, and original Source80 full1043
remains historical actual1. No current original full/sanitizer result, push,
Actions enablement, skipped security/TDE work or filtered restart is claimed.
