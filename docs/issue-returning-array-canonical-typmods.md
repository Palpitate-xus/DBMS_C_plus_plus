# RETURNING arrays retain physical element modifiers

The physical origin adapter resolves a direct RETURNING column independently
of its output alias. Its type guard nevertheless compared raw type spellings:
`character varying[]` from the typed mutation versus `varchar[]` from storage.
The guard failed and the already validated physical VARCHAR/CHAR element
modifier was lost, producing -1 instead of 8/7.

This independent change compares array identities through the shared canonical
type resolver. Scalar matching remains unchanged. Only a genuine AST-resolved
physical source column can inherit its catalog modifier; a computed expression
with a colliding alias still has no physical column origin.

`source-consumer-whole-v3.log` (94285) retains the actual Simple INSERT/UPDATE
RETURNING modifier failures. The unchanged full origin fixture and 24-base /
nine-shape controls pass in `source-consumer-wire-v5-final.log` (30845, exit 0),
including Simple plus Statement/Portal Describe, quoted alias collisions,
TEMP/public shadowing, empty and NULL rows. Both whole strict PostgreSQL 180006
reference logs pass. Fifteen matching native controls also pass (39350).

Normal O2 build 28790 has all 58 source/header/flag receipts and stamp; SHA256
`4fc50a6a94ad488d783a0e06b2d113cc94639c2a50d6bab1a1de629429b6e64d`.
Artifacts are in `/tmp/dbms-physical-array-source-consumer.PkJ5r6tY`. No modifier
is guessed from result data or removed from the expected protocol contract.
