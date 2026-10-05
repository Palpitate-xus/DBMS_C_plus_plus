# STO-06: TOAST format and storage policy remain partial

## Findings

The current implementation provides a working custom overflow store, but it
is not PostgreSQL's varlena/TOAST representation. Large variable-length values
are replaced in heap rows by the textual marker `__TOAST__<id>` and stored in a
sidecar chunk heap plus B+ tree. Compression uses zlib `compress2` and is kept
only when it strictly shrinks the input. There is no PGLZ/LZ4 selection,
short-varlena representation, PostgreSQL external pointer, or deduplication.

The current schema has no per-column TOAST storage-policy field or
`toast_tuple_target` setting. `pg_attribute.attstorage` is synthesized as
`x` for any variable-length column and `p` otherwise, so users cannot choose
the PostgreSQL `PLAIN`/`EXTERNAL`/`MAIN`/`EXTENDED` behavior through column
storage options.

No new data-corruption defect was established in this audit. Existing code
does bound decompression by the declared column limit, validates chunk
identity/order/metadata and index-to-heap references, cleans newly allocated
chunks on rejected writes, and defers orphan reclamation while database
transactions may still need old tuple versions.

## Evidence and verification

Source inspection covered `src/commands/TableManage.cpp` (chunk encoding,
compression, reads, cleanup and vacuum), `src/commands/TableManage.h`
(threshold and storage API), and `src/commands/DdlExecutor.cpp`
(`attstorage` derivation). Focused tests passed:

- `bash scripts/build_one_test.sh toast_test` — compression, large values,
  failed-write cleanup, concurrent ID allocation, marker-shaped user values,
  corruption rejection, and committed-value recovery.
- `bash scripts/build_one_test.sh vacuum_toast_test` — old-snapshot safety and
  orphan cleanup.
- `bash scripts/build_one_test.sh bytea_large_toast_test` — values over 64 KiB
  across insert, update, and reopen.

No code changed in this audit. The full registered suite was not rerun on this
revision, and no PostgreSQL 18.6 runtime differential was run.

## Status

STO-06 remains partial. PostgreSQL-compatible varlena/external-pointer layout,
column storage strategy, configurable target, PGLZ/LZ4, TOAST deduplication,
and complete transactional/WAL integration remain open; this audit does not
claim those capabilities are implemented.
