# Pure binding of an UPDATE target's actual default

The old ordinary consumer bypassed typed UPDATE when any SET expression was
DEFAULT; retained WITH UPDATE explicitly rejected DEFAULT with 0A000. The new
QueryBindingMetadata.updateDefault callback receives the resolved relation schema,
name and target column. It copies the actual target definition without invoking
a function, reading a row or opening a child. No definition means assignment NULL.

The engine callback uses the physical target's proven default origin. An explicit
column default (including NULL) wins; a Domain origin uses the direct domain's
current own default. LegacyFrozen remains frozen. This work depends on the prior
domain-origin/schema B foundation and does not reinterpret legacy 9/10 flags.
Materialized targets have no column-default storage: their actual kind supplies
no definition, after which the existing whole-analysis/planning read-only guard
reports 42809. Unknown ordinary physical metadata remains an error, not a guessed
NULL. Populated and unfilled MV DEFAULT controls preserve target 42809 versus
source no-data 55000, with no mutations or routine calls.

The binder parses the stored value expression, not a rendered mutation. It binds
that live AST in an independent empty SQL/PL namespace under the real function
metadata provider, then supplies an implicit assignment conversion to the actual
target type. Caller row/CTE/procedural names cannot be captured. Native arbitrary
subquery defaults are rejected 0A000; they are not supported SQL defaults. Genuine
function return metadata and execution-owned expression sites are retained.

The implicit conversion matters: the expanded strict180006 reference assigns
numeric 1.9/-1.5 to integer 2/-2, while candidate V1 returned 22023. The corrected
AST uses the existing implicit cast semantics, including typed NULL, rather than
turning a rendered value into an integer parser input. Pure metadata tests verify
resolved callback components, genuine routine AST type, no parameters/rows and
rejection of caller-name capture.

This appends a public std::function field to QueryBindingMetadata. It adds no TU
or AST field, but requires a consistent full 58-TU/header build at integration.
The isolated new-header all58 O0 epoch and later source-matched CPP rebuilds are
not evidence for normal O2, sanitizing every TU or the full repository gate.

The actual runtime activation and its original full protocol matrix are in the
separate UPDATE DEFAULT consumer change. VIEW/generated/identity/default-source
identity freezing, general assignment casts and the complete DEFAULT/DML families
are not closed by this metadata foundation.
