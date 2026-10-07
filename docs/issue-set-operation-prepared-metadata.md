# Whole set-query prepared metadata

Scope: no-parameter whole set queries at protocol Parse, statement Describe,
and portal Describe. This does not close set-clause execution or parameter
inference, UNION DISTINCT/INTERSECT/EXCEPT execution, domains, or the whole
query family.

The network descriptor adapter previously rejected every `setOp` shape. The
actual binder already computed common output types and transformed UNKNOWN
inputs, but Parse did not invoke it and Describe published NoData. The new
adapter consumes `StorageEngine::prepareBoundQuery().output` without opening
a source, calling a routine, planning runtime arithmetic, or executing a
target. Set outputs deliberately have zero table/attribute origin. Parse
performs this analysis before publishing a named statement; failed analysis
does not leave a prepared name behind.

The original complete 18-case script and all original expectations remain.
Added portal descriptors and failed-name `26000` controls strengthen it; the
metadata writer must still leave `currval` undefined (`55000`). It is now a
default registered script, not a filtered known-green mode.

## Reproduction and proof

Private source parent ROOT `25ee2f2517ac62b5d68d660df96c22ca6af67880`.
Artifacts: `/tmp/dbms-set-operation-metadata.lvcm1DcI`.

* Strict PostgreSQL `180006` complete original and expanded matrices: both 0,
  `reference18.original.log` / `reference18.expanded.log`.
* Matching baseline `2dd7fdd247851a62cff828941bf20c4942b658d7a2d97336024a8fa447817452`:
  `baseline.original.log`, actual exit 1, all 12 missing descriptors and all
  6 missing analysis errors preserved. Metadata had no writer effect.
* Candidate `8c8cf369d160fce739a841c2c7b912cbcbf81d70704e49bd6eb18b6a9f419b10`:
  complete expanded script exit 0 (`candidate-v1.whole.log`). Three matching
  native tests (set binding, parsed expression types, typed Append) exit 0.
* Seven complete serial adjacent scripts exit 0 (`adjacent.corrected.log`):
  both SRF matrices, retained Append, structured sets, quantified demand,
  FROM-less demand, and PL whole-query binding. An earlier wrapper used five
  nonexistent filenames; `adjacent.log` is preserved and is not a SQL pass.

The object epoch is the genuinely fresh 58-TU SRF public-header group, with
ROOT's changed TM and TxnIdGenerator freshly compiled for this parent, then
the candidate Network TU freshly compiled. All reused source/header/flag
signatures and final source/header/object manifests match. Private objects
are `-O0`; this is not a formal full-O2 or full-suite claim. No public layout,
signature, or TU count changes occur in this fix.

Set ORDER/LIMIT/FETCH ownership remains independently open: the current
parser places unparenthesized trailing clauses on the RHS, and the typed
ordinary entry intentionally declines those shapes. Prepared parameters
also retain their existing consumer; this commit does not invent unknown
parameter types from values.
