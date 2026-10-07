# Prepared quantified cursor acquisition follows the compiled root

The original-site registry records every quantified comparison cloned from a
bound expression. It is not a runtime demand list. `prepareChildCursors()` used
that registry even after constant CASE/Boolean planning had removed a site.
Constructing the discarded child's real typed plan could consequently raise a
constant error that PostgreSQL does not reach.

The decisive native regression retains this SQL:

```sql
SELECT CASE WHEN false
  THEN 1=ANY(SELECT CASE WHEN true THEN 1/0 ELSE 1 END)
  ELSE true END;
```

With the old PCE object, the genuine child factory is called once and graph
construction raises `22012`; the native assertion exits 134. With the new
object, it is never called and the result is true, both with default CASE
preparation and with explicit statement constant planning.

The fix walks the actual execution-owned compiled roots registered by
`prepareExpression()`, preserving each reached quantified node's mapping to its
original AST site. Only those sites acquire cursors. Whole-query binding is
unchanged: an unknown column or malformed unknown-input cast in a dead branch
still fails analysis. Reached identical-looking SELECT sites keep separate
cursors; repeated preparation does not duplicate graphs; replacing the cursor
factory still clears old cursor/memo state.

Evidence retained under
`/tmp/dbms-explain-root-projectset.ahA83RRE/quantified-demand/`:

- `baseline.log`: old-object native exit 134, `creates=1 state=22012`.
- `candidate.log`: dedicated native passes all dead/reached-site and static
  analysis controls.
- Four matching adjacent natives pass: ProjectSet typed child execution,
  quantified execution, constant planning, and prepared query cursor.
- `headers.audit` and `other57.audit`: the candidate changes only the PCE
  production object relative to an independently fresh 58-object O0 group;
  headers, other production sources, and build flags match. This is not a
  ROOT/formal O2 combination claim.

The wider ProjectSet matrix retains its separate ordinary bare-SRF receiver
failures. This change does not implement multi-SRF projection, physical-source
ProjectSet, correlated CTE restart, or a complete planner/cost model, and does
not close those families.
