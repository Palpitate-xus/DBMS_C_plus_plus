# STO-03 partial: heap page identity

## Finding

`PgPage::init(pageId)` previously discarded `pageId`. The on-page checksum
covered the bytes in a page but not its expected file block number, and both
the buffer-pool validator and offline heap verifier called `isValid()` without
an expected block identity. Swapping two structurally valid, checksummed pages
therefore passed validation and exposed each page's rows at the other block's
row IDs.

## Change

Page layout v5 stores the physical block number in the existing 4-byte header
slot and sets `PD_PAGE_ID_BOUND`. The normal page checksum covers both fields.
BufferPool validation and `--verify-data-checksums` now compare that identity to
the physical block number and fail closed on a mismatch. Offline verification
reports `identity-bound-blocks` and `legacy-identity-unbound-blocks` separately.

Layout v4 pages remain readable and read-only loads do not modify them. On the
first dirty mark, `PageAllocator::markDirty` binds the physical block ID and
lets the normal writeback path persist the v5 page. This preserves data-file
readability without claiming that an old unbound page can retrospectively
prove it was never swapped before upgrade.

## Verification

- `checksum_test`: checksum corruption rejection, swapped-page rejection, and
  v4 read compatibility followed by first-write identity migration all pass
  after rebuilding the corrected test.
- `checksum_verification_e2e_test.py`: clean v5 pages verify and page
  corruption is diagnosed; a v4 page is reported as identity-unbound without
  being modified; pass.
- `scripts/build_tests.sh`: the full run on the initial page-identity revision
  exited 1 because `checksum_test` compared an inserted NUL-terminated byte
  string against one omitting the NUL. The assertion was corrected and the
  migration was then narrowed from read-time to dirty-mark-time; the full suite
  was not rerun on that final variant. The corrected focused C++ test, final
  production build, and checksum E2E pass. Other registered tests in the full
  run passed; TLS timeout E2E skipped under the configured TLS stub.

## Remaining scope

STO-03 remains partial. Pages still use the project's Fletcher-16 checksum,
not PostgreSQL's checksum contract; tuple/varlena/TOAST representation,
PostgreSQL special-space semantics, and WAL LSN compatibility are not aligned.
Old v4 pages have no block identity until first dirty mark, so offline verification
reports rather than detects historical swaps of those pages. No PostgreSQL
18.6 runtime differential was performed.
