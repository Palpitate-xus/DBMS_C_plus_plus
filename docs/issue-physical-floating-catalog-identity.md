# Physical floating codec and catalog identity

The four-byte REAL factory deliberately stores the legacy codec name `float`.
Catalog registration interpreted that spelling as bare SQL FLOAT/float8, so
REAL[] preparation acquired `double precision[]`; the actual typed REAL cell
then failed the descriptor guard. The eight-byte factory's legacy `double`
spelling also missed the builtin OID map and allocated a user OID (10001 in the
retained native diagnostic) instead of float8 701.

The catalog registrar now maps those two genuine declared physical layouts to
float4 700/float8 701. It does not change either storage codec spelling or infer
an element type from a row, NULL, column label or SQL keyword. Arrays retain
their existing attndims representation; their protocol descriptors remain
array OIDs 1021/1022 with variable width.

Evidence in `/tmp/dbms-physical-array-source-consumer.PkJ5r6tY`:

- `real-native-baseline-v3.log` (23317, exit 134): actual prepared REAL[] type
  is double precision[], while physical codec remains float.
- `real-native-catalog-identity-v4.log` (80800, exit 134): float4 correction
  exposes the independent legacy double OID 10001; all OID guards are retained.
- `source-consumer-native-v5-final.log` (39350, exit 0): new actual REAL[] /
  DOUBLE[] / scalar REAL catalog and typed execution guards plus fourteen
  controls pass. Non-NULL arrays and scalar 1.5 retain their actual values;
  SQL NULL is checked by bitmap, not payload text.
- `source-consumer-wire-v5-final.log` (30845, exit 0): all twelve whole fixtures
  pass, including the unchanged 24-base array matrix and original nine shapes.
  `full-element-reference18-final.log` independently passes strictly 180006.
- Normal O2 28790 and all 58 source/header/flag receipts/stamp pass, SHA256
  `4fc50a6a94ad488d783a0e06b2d113cc94639c2a50d6bab1a1de629429b6e64d`.

Two initial new-native fixture mistakes (catalog API names, then counting
negative system attributes as user columns) are retained and not described as
product fixes. A transient uncommitted candidate changing the float codec name
was discarded without runtime proof; the final patch leaves TableManage.cpp
byte unchanged. Existing persisted incorrect attributes need normal catalog
reconciliation; this is not a blanket legacy-catalog migration or all-floating
type/codec family completion claim.
