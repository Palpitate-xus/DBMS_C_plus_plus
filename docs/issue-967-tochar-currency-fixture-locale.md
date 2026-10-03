# Issue 967 — isolate the locale-sensitive numeric `to_char` case

## Finding

The 462-case PostgreSQL 18.6 differential run had one mismatch in `to_char_numeric`: `to_char(482, 'L9999')` returned `$  482` on PostgreSQL and three spaces plus `482` on the DBMS. The reference database used `en_US.utf8`, while the local DBMS session defaulted to `C`. The `L` pattern is intentionally controlled by `lc_monetary`, so the case compared different settings rather than equivalent behavior.

## Change

`tests/compat/cases/to_char_numeric.sql` now sets `lc_monetary='en_US.utf8'` before the currency-symbol assertion and restores `C` afterward. This keeps the locale-sensitive assertion explicit and isolated on both endpoints; no product code changed.

## Verification

- Test-fixture commit: `fb5cb77e` (`test(compat): fix numeric currency locale setup`).
- PostgreSQL 18.6 focused differential, `to_char_numeric`: `cases=1 failed=0`.
- The prior full-differential mismatch is recorded in issue 966; a new full 462-case run has not yet been made after this fixture correction.
- `git diff --check` — passed.

P0-16 remains `partial`; correcting this test setup does not close the broader compatibility-validation gap. No push was performed.
