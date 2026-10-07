# Long default parser fixture uses the actual footer

The unchanged long_default_format_test exits134 on the immutable pre-domain-fix
V6 endpoint (long-default-baseline-build.log): it locates DFT1 from EOF, but the
current physical schema10 also appends RLB1/RID1. Its corrupt.back() changes an
identity byte, not an SQL default byte. Both original failures are retained.

This independent test-only correction validates version10's empty range footer
and nonzero RID before locating the DFT1 entries. Its NUL check corrupts the
actual expression payload. Excessive length, invalid column, duplicate column,
truncation, trailing bytes and the legacy-prefix checks all remain. Already-open
parser ownership avoids constructor-time WAL recovery masking parser assertions;
the real current schema is restored before DROP.

The explicit schema9 legacy-prefix image proves parser acceptance, not cold
schema10-to9 recovery compatibility. New domain-origin B plus long DFT1/DSO1/RID1
has separate native coverage. Corrected final native group 42608 and refreshed
ASan/UBSan drivers 19547 pass, while the production default/format changes remain
separate. No SQLSTATE, row or timeout expectation was weakened.
