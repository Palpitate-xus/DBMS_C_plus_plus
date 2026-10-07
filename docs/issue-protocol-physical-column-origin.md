# Protocol descriptors need the actual logical source and projection ordinal

Independent NetworkServer metadata repair after 6f537f86. All baseline,
intermediate failures and full strong expectations remain retained. No public
API/layout/source-count change (58 translation units).

Old Simple descriptions try the logical TEMP name as a physical filename;
Extended Describe then synthesizes SELECT * from the private physical TEMP
filename and cannot find its logical catalog entry. Both paths lose relation
identity and modifiers. Looking up output labels additionally associates a
computed or renamed projection with the wrong physical attribute.

The adapter now reads the parser's actual single base range / DML target and
projection AST, resolves TEMP/search_path/explicit schema identity from a
copied metadata snapshot and follows catalog namespace shadowing. It does
not execute a query, inspect datum bytes or invoke the execution name resolver.
Direct ColumnRefs and stars retain positional physical origins; computed
projections cannot inherit them from their labels. Original SQL replaces the
synthetic physical-filename SELECT for Statement/Portal Describe. Direct
RETURNING target projections keep the same target metadata path (including
image qualifiers); CTEs/views must not borrow a shadowed physical table.

The original matched normal-O2 baseline 967ed6a3 has 70 failures in the full
temporary-element/broad-array matrix. The first candidate d2e4d2c and final
candidate 5e806d68 both reduce those to exactly six unchanged expectations,
all due solely to missing TIMETZ[] OID1270 (actual25). Every core length,
numeric precision, NULL/empty, alias, quote, star/mixed and physical-origin
check passes; the other 23 broad array base OIDs match strict 180006. Both
complete strict-reference matrices pass unchanged.

Final normal-O2 build41916, repeat, all58 source/header/flags/object signatures
and stamp passed. Immutable source/objects/binary archive:
`/tmp/dbms-physical-array-typmod.YCjPAKiX/origin-v2-immutable.oI4OgAqI/`
SHA256 `5e806d68396b1ef8f66cfb3ad4cea1ff77ece7ca36f25d65037d0157b5f0ae63`.
All eight full adjacent scripts57308 passed: unchanged nine-shape arrays,
element modifiers, RETURNING arrays/transition binding, character Describe,
WITH transition RETURNING, quoted arithmetic Describe and typed view triggers.

Expanded physical_column_origin_protocol_e2e_test.py remains deliberately
unregistered while a separate genuine runtime problem is repaired: Simple
SELECT through quoted uppercase source alias "A" returns42P01 although strict
180006 succeeds. Its old967 and new5e806 runs both retain that error; preceding
TEMP/public-shadow and explicit pg_temp origin checks now pass. The complete
12-shape matrix is also initially unregistered because the six TIMETZ failures
remain real, not converted into green assertions. No full-family closure is
claimed; general CTE/JOIN/VIEW Describe output lowering is not implemented by
this narrow legacy adapter. No new array-value codec correctness is claimed.

Artifacts and exact failed/passing logs are in
`/tmp/dbms-physical-array-typmod.YCjPAKiX/`. Default15s is unchanged. Each server
is stopped in finally; tmpfs semantic tests do not establish disk performance.
