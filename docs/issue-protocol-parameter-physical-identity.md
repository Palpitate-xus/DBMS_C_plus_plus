# Bind preserves the declared physical parameter type

The existing SQL-lowering path cast only selected NULL/bare VALUES parameters.
A non-NULL SMALLINT became INTEGER; NULL VARCHAR/array inputs became UNKNOWN.
The initial pg_typeof candidate records those failures. Candidate43092 exits1;
its logs and frozen binary are retained. After preserving typed lowering,
candidate20317 still exits1 at three real Bind overflow controls (the separate
unchanged fromless fixture also fails at bare USER). No expectation is relaxed.

Use Parse's actual OID to select a copied catalog row and its actual namespace,
and lower both NULL and non-NULL values through that qualified physical name.
Do not reconstruct a type from runtime bytes or a truncated SQL alias table.
Physical BIT/CHARACTER declarations do not inherit SQL keywords' omitted
length-one typmod. Use the existing signed integer input codec at Bind for
the actual 16/32/64-bit width, retaining 22003, integer bases/separators and
the minimum BIGINT value. OID's separate legacy input is not reclassified.

The genuine verified PostgreSQL18.6/server180006 passes all18 new controls:
NULL/non-NULL, text/binary integer, preserved Describe identities, arrays,
VARCHAR/NAME, no-default-length BIT/bpchar, input syntax and three overflow
errors. Old candidatec5c9b2 passes10 descriptor-only controls but fails the
three overflows; expanded16 controls record5 failures. These are retained
reproductions, not successful validation of the repair.

Normal own NetworkServer units were compiled independently (30430,12695,
21036,36345); no headers or layout changed. Candidate23111 exits1 with four
wire failures; candidate67366 exits1 with two, including nine remaining
INTEGER-BETWEEN error-phase differences. Retain both matrices. After the
separate nullable CAST repair, closed Bind planning resolves only the genuine
allowed primitive OIDs from copied metadata; qualified generated casts are
eligible without admitting routines, columns, parameters, domains or SQL children.

Final matching publication85985 actually exits1: all11native and14/15wire
entries pass. Whole pg_typeof56, parameter18, TEXT-width, BIT-input42,
INTEGER-BETWEEN350, BIT-BETWEEN250 and complete original default protocol
all pass. The only failed entry is the unchanged original fromless fixture
at the separate bare USER grammar failure42703. All58 own-object receipts,
normal/repeat/cache/source/frozen start and terminal checks0. This is not a
complete registered-suite pass or closure of all original protocol requirements.
Frozen44074863ae7524fe; source seal
`d0f95be62a998eaed01e5a8dd22aaba73e258e24bcabe5f910d4b517b0abc2fc`.
Original273 requirements and15 user-deferred security/TDE items remain;
no push or Actions activation.

Artifacts: `/tmp/dbms-having-grammar.MPk64yc0`.
Final log: `pg-typeof-bind-planning-publication11-15.log`.
Reference: `parameter-type-physical-reference18.log`.
