# version() uses the ordinary scalar namespace and static TEXT declaration

After USER was repaired, the unchanged mixed fromless fixture reaches the
next missing callee: version()42883. The actual preceding frozen8160f9 binary
also fails six of the new16 controls. Its isolated Main string shortcut was
not a general scalar implementation and ignored ordinary routine resolution.

Register a real zero-positional/zero-named-argument builtin with stable
volatility and TEXT result metadata. Resolve namespaces/signatures through the
existing scalar binder; OVER/aggregate misuse stays42809 and invalid arity
stays42883, including undemanded WHERE-false projections. Return the actual
project banner from common/version.h, not a fabricated PostgreSQL release.
Remove Main's special constant output and bypass its legacy same-named UDF
shortcut so real implicit/explicit pg_catalog precedence and user schemas
choose the ordinary bound routine.

Actual verified PostgreSQL18.6/server180006 passes all16 structural/type/error/
namespace controls; branding checks intentionally distinguish the reference's
PostgreSQL18 banner from this project's DBMS-C++ banner. Own normal expression
and Main builds50779/63496 both0; matching normal O2 native24895 passes actual
banner, metadata, volatility, signature and independent-engine mixed binding.
No header/layout change since the separately rebuilt USER generation.

Matching publication98287 actually exits0: all14native and16wire entries pass,
including new16 version controls, expanded keyword41, pg_typeof56, parameter18,
unchanged full fromless, TEXT-width113, BIT-input42, INTEGER-BETWEEN350,
BIT-BETWEEN250 and the complete default protocol. All58 own-object receipts,
normal/repeat/cache/source/frozen start+terminal checks0. Frozen SHA256
`ee1d8c5d2caa68305011393c60cc822a8e1314e966d4b8b836067edad68acf34`;
source seal `635204bdab3d28c704d3574cbfae64d748d72b4b8d49b6c2585b1b246ffd16a0`.
This is a focused publication, not a complete registered-suite pass.
Earlier failures are retained. Original273 statuses
and15 user-deferred security/TDE requirements unchanged; no push or Actions.

Artifacts: `/tmp/dbms-having-grammar.MPk64yc0`.
Baseline: `version-function-baseline816.log`.
Reference: `version-function-reference18.log`.
Compile: `version-function-own-expression-build.log`, `version-function-own-main-build.log`.
Native: `version-function-native.log`.
Current publication: `version-function-publication14-16.log`.
