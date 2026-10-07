# Native schema parser fixtures versus cold recovery

The original schema_format_test fails on the immutable pre-domain-fix V6
endpoint during cold recovery after chopping a mandatory RID1 suffix. Its
actual exit134 log schema-baseline-build.log remains preserved.

This independent test-only correction validates the real schema10 footer layout
before constructing explicit schema9 parser images. Every prior negative remains:
negative/excessive width, eight-byte truncation and unsupported magic.
Already-open owners exercise the parser;
an independent cold-start corrupt rejection checks the stronger recovery guard.
The real schema including SID/RID identity is restored before DROP.

These tests demonstrate old-layout parser acceptance, not a fabricated
schema10-to9 cold recovery compatibility claim. Domain-origin B and DFT1/DSO1/RID1
coexistence have independent native coverage. Production parser/default changes
are in separate commits; this fixture correction does not alter SQL/state/row
expectations or weaken the cold recovery guard. The corrected native passes in
the final eight-native group 42608 and scoped sanitizer group 14525. The distinct
long-default fixture correction and its negative payload checks have their own
commit and evidence.
