# Physical array protocol descriptors

Status: this independent physical-column descriptor issue is fixed and verified.
This is not a claim that every array type, array operator, or binary array codec
is supported.

## Reproduction and cause

On the exact `a7d460c7` source, normal O0 binary
`7bad59eb7b5c671b090ebfff8ca49b3e3a63e04b03e2dfb8c895932cd96abb56`,
the new unchanged physical-array protocol matrix failed 52 metadata assertions.
Values were correctly returned, but plain physical projections and extended
Describe paths lost `Column::isArray` and published element OIDs/lengths.
The physical catalog override could also replace an array declaration with the
project catalog's element OID plus `attndims`. This affected empty tables and
WHERE FALSE as well as nonempty results; no value-based inference is involved.

The fix preserves the schema's declared array identity in main's projection
descriptors and NetworkServer's metadata-only Describe/type hints. Arrays have
variable protocol length, and a scalar catalog attribute cannot overwrite their
array OID or length. The missing built-in `bool[]`/`boolean[]` mapping is OID
1000. Scalar boolean remains OID 16. A first candidate correctly fixed the other
types but retained nine BOOL-array metadata failures; that failed log is kept.

## Evidence

All artifacts are retained under
`/tmp/dbms-physical-array-descriptor.TsQVhYFr`:

| Check | Actual result / artifact |
| --- | --- |
| Exact a7 baseline | exit 1, 52 metadata failures, `baseline-a7.log` |
| V1 candidate | exit 1, nine BOOL-array OID failures, `candidate-v1.log` |
| Final candidate | exit 0, `candidate-v2-strong.log` |
| Strict PostgreSQL 18.6 XML/en_US oracle | exit 0; runtime version exactly 180006, `reference-18-permanent-final.log` |
| Seven matching native tests | exit 0, `native-v2-7-canonical.log` |
| Eight serial wire scripts | exit 0 and no failed scripts, `adjacent-v2-final-8.log` |
| Official repeat / 58 source-header-flags signatures / stamp | exit 0, `build-v2-final-audit.log` and `freeze-v2-final.log` |

The final immutable binary is
`candidate-v2-immutable.v0mr581B/dbms_main.frozen`, SHA256
`35f35fe868318301c6482acdac14a82a0ef48dd88d910dfc032bce6e8fdc1ab7`.
This is normal O0, not a production O2 claim. All 58 inputs match the private
build receipts: exact matching immutable a7 objects were audited before reuse,
and the three changed production CPPs were freshly compiled. There are no new
public headers, ABI fields, or translation units.

The permanent wire test checks simple results and both statement/portal
RowDescriptions from Parse/Describe/Bind/Describe/Execute/Sync, including actual
row values and NULL cells. It covers INT[], TEXT[], BIGINT[], BOOL[], two
dimensions, quoted columns, aliases, mixed scalar projections, stars, sorting,
WHERE FALSE and initially empty tables. Reference objects are transaction-owned
TEMP tables and are rolled back. Default 15-second deadlines are unchanged.

The native OID test's first new fixture incorrectly called the canonical-name
map with uppercase `BOOL[]` directly and failed (exit 134,
`native-v2-7.log`). The corrected fixture explicitly canonicalizes SQL spelling
before calling that existing exact-name map; the protocol SQL and all original
OID/payload assertions were preserved.

The seven native controls include prepared array metadata, array expressions,
type registry, prepared execution, and the original explicit-host WHERE/ORDER
plans. The eight wire controls include the new matrix, typed array concat,
RETURNING arrays, scalar cardinality/types, all 84 quantified controls, typed
EXPLAIN, PL namespace binding, and SELECT INTO execution demand. No complete
default protocol or all-array-family completion is claimed here.
