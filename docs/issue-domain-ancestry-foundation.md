# TYPE-19: physical columns over nested domains

Scope: nested builtin scalar domain declarations and physical table columns.
The original unfiltered priority setup is retained verbatim in the whole
protocol test. This is not closure of the domain/type family.

## Original failure and contract

At exact source `75039dd0664eb2495456ca75902e9197b086f2a4`, creating a TEXT
domain and a domain over it succeeds, but declaring the nested domain in a
TEMP table fails with XX000/unknown type. `columnDefToColumn` only followed one
parent and then treated the parent's domain name as a physical builtin.

Resolution now follows actual canonical identities through all parents before
choosing physical storage. Every ancestor's NOT NULL is retained separately
from the column's own NULL property, every ancestor CHECK is combined with all
column CHECKs, and the nearest explicit domain DEFAULT wins. Explicit DEFAULT
NULL is distinguished from no default. Builtin modifiers are retained. The
outer declared type and its real catalog OID are never replaced by TEXT or an
anonymous base type.

CHECK's VALUE references are bound through metadata-only query preparation and
source positions; string literals, function/type names and quoted unrelated
identifiers are not text-replaced. Quoted/schema identities and creation-time
parent identity survive search-path differences. Cycles fail XX001 and the
explicit supported ancestry depth is 64 (exceeding it fails 54001).

## Persistence and reads

No Column/TableSchema binary format or physical relation bytes were changed.
The pre-existing logical `Column.domainName` was never written by binary schema
serialization. A narrow copied catalog getter reconstructs it from the real
pg_attribute domain OID on cold, cached and transaction-schema reads. It does
not execute SQL or infer identity from stored values/CHECK text.

Old five-field `.domains` records and old eight-field pg_type text records
remain readable. New explicit versioned text fields persist domain default
presence/NOT NULL and catalog ancestry/modifiers. New `.domains` fields are
hex-encoded so CHECK/default literals containing separators remain intact.
Malformed new versions, flags, fields and embedded NULs fail closed. Rewrites
preserve untouched old records. These are logical text metadata additions, not
automatic binary migration.

Catalog attributes retain the outer domain OID. Protocol value descriptors
unwrap copied type ancestry to the actual scalar type and nearest modifier;
CHAR(3) is OID1042/typmod7 and NUMERIC(6,2) is OID1700/typmod393222 in Simple,
Statement Describe and Portal Describe, including zero-row results.

## Retained evidence and boundaries

Artifacts are in `/tmp/dbms-domain-ancestry.riCfkRa3`.

- Original setup: `setup-baseline.log` (actual failure) and
  `setup-reference18.log` (actual strict 180006 success).
- Expanded unchanged whole oracle: `domain-reference18-v3.log`, exit 0 on the
  owned XML/en_US/libc PostgreSQL 18.6 reference. No PG17 relabeling or deadline
  increase was used.
- V1 whole candidate `77127` retained six real NOT NULL/Describe failures;
  V2 whole `32557` passed. V3 missing-header compilation and earlier native
  harness filename/API failures are retained, never counted as runtime passes.
- Corrected V6 complete eight-native group `22489` passed, including cold
  catalog/reopen, cache hits, actual catalog OIDs, malformed metadata cleanup,
  domain full/multiple CHECKs, array modifiers, catalog failure and FK controls.
- Scoped ASan/UBSan `26739` passed after freshly instrumenting TableManage,
  CatalogManager, CatalogService and parser plus the storage native/stubs. The
  other matched production objects were O0, not instrumented: no full-ASan
  claim. Earlier `55501` found the genuine exception-owned INSERT leak/exit UAF.
  That owner fix is a separate preceding commit, not bundled into this issue.
- V5 whole adjacent wire group retained 7/8 successes and the quoted array
  origin 42P01 failure. The same unchanged test fails at the same SQL on exact
  750 baseline `35727`. Final verification explicitly adds the existing ROOT
  dependencies b72e27aa (RETURNING modifiers), c8e9a83b (boolean DELETE receiver)
  and b140ac13 (genuine physical array AST consumer), without adopting newer
  incompatible public headers or weakening that test.
- The V7 whole group then exposed the missing fourth existing dependency:
  REAL[] catalog OID1022 instead of1021 and a NULL source/descriptor mismatch
  after the genuine AST consumer became active. `wire-final-v7-dependencies.log`
  retains that actual failure. V8 explicitly adds ROOT 1a7ce3ec's real physical
  FLOAT4/FLOAT8 catalog identity fix; no ROW values or expected OIDs are changed.

Final V8 proof uses exact 750 plus the separately committed INSERT owner and
those four explicit ROOT array dependencies, with this domain patch. It does
not use current ROOT's newer public headers/physical-relation/SRF layouts.
`candidate-v8-full-dependencies-O0.log`: official manifest/all 58 source and
header signatures/private O0 flags/cache stamp, build and normal repeat exit 0.
Executable SHA256:
`d67d38b17f2b4c878db6872b3804a6960e3a118a2b3fff4a7faa658e6da560b1`.
`native-final-v8-all-dependencies.log` (87813): all nine matching natives exit 0.
`wire-final-v8-all-dependencies.log` (42145): all unchanged eight serial whole
scripts exit 0/FAILURES empty, owned servers finally stopped. This includes the
previously failing complete 24-base array and quoted-origin fixtures, not a
reduced green mode. Protocol temp directories used `/dev/shm` with unchanged
15-second deadlines; the complete native groups also passed on real `/tmp`.
`domain-reference18-final-v8.log`: the complete current whole domain fixture
again exits 0 against actual strict 180006.

These are private matching proofs, not a current ROOT normal-O2/full-suite,
all-PostgreSQL differential, or disk I/O/performance closure claim. ROOT must
preserve its newer schema identity/provider fields on merge and rebuild its
entire current public-header group before claiming a combined result.

## Explicitly open independent followups

`followups-reference18.log` and `followups-candidate-v2.log` retain real failures:
ALTER DOMAIN SET DEFAULT after a physical column already exists yields stale
default 1 instead of 2; domain-to-base FK declaration is incorrectly rejected.
ALTER COLUMN TYPE into/out of a domain needs its own continued NULL/check/OID
controls. They are not closed by initial-declaration success.

Dependent DROP DOMAIN CASCADE, domain arrays, live constraint alteration,
renaming/dependency propagation, full domain expression/routine/cast identities,
legacy ambiguous default origin, and complete named diagnostic/catalog fields
remain separate unsupported or unverified boundaries. No all-TYPE19 or
all-array-family completion is asserted.
