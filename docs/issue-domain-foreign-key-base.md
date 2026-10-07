# Domain foreign keys compare their resolved base types

The immutable scalar-domain foundation rejected an INT foreign key pointing
to an INT-domain primary key, the reverse direction, two distinct INT
domains, a nested domain, and a mixed composite key. All five declarations
are valid on strict PostgreSQL 18.6; plain INT was the positive control.
The old nominal domainName equality check returned before the existing base
type compatibility decision. Domains keep their own identity and constraints;
those names are not the foreign key equality operator's type identity.

The repair removes that early nominal rejection. The existing scalar/array,
named-enum and resolved physical-base compatibility guards stay unchanged.
No public API, schema format, Column layout or domain catalog changes.

## Evidence

Artifacts: `/tmp/dbms-domain-fk-base.B9RaA78f/`.

- Original `baseline-native.log`: actual assertion/exit134 at the valid
  INT-to-domain declaration. `baseline-wire.log`: all five domain cases
  failed CREATE with XX000, while the original plain INT case passed.
- Exact original six-case fixture and strict180006 reference are retained.
  ROOT independently reran `root-reference18-original-six.log`, terminal0,
  and `root-whole-original-six.log`, terminal0. CHECK23514, NOTNULL23502,
  FK23503, parent delete rejection, values and output OIDs remain asserted.
- ROOT `root-native-six.log`, terminal0: new domain FK native, original
  definition validation/referenced column/insert SQLSTATE/self insert and
  domain ancestry storage tests. Identity and constraints remain intact.
- ROOT `root-complete-adjacent-six.log`, terminal0: original whole domain FK,
  domain ancestry, FK insert/modify/statement visibility/self-insert scripts.
  Runtime candidate uses memory-backed TMPDIR with unchanged default15s;
  this is a semantic proof, not proof of disk-backed latency.
- ROOT `root-source-header-object-audit.log`, terminal0: fresh TableManage
  with 57 byte-identical archived domain-foundation objects; every current
  source/header/flag/object receipt, repeat stamp and immutable binary agree.
  SHA256 `8cdf8e029141e35bf8b07663977d40f5a3ed911abf2d37169ee1c1d78b35efe2`.
  This is a matched incremental O0 epoch, not a fresh all58 O2 build, current
  ROOT combination, full suite or sanitizer proof.

Domain ALTER/revalidation, broader implicit cast/operator selection, domain
arrays/composites and general FK concurrency remain required separate work.
No failed original assertion was removed and no full-family closure claimed.
