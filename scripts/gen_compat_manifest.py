#!/usr/bin/env python3
"""Generate tests/compat/manifest.yaml from docs/postgresql-18-gap-audit.md.

The manifest is the machine-readable source of truth for the 273 gap IDs
(16 P0 + 243 capability gaps + 14 divergences).  CI and completion tooling
read it; the audit document remains the human-readable authority.

Usage: python3 scripts/gen_compat_manifest.py
"""
import os
import re

SRC = "docs/postgresql-18-gap-audit.md"
DST = "tests/compat/manifest.yaml"

FAMILIES = [
    "P0", "SQL", "CAT", "TYPE", "FUNC", "CONS", "DML", "QRY", "OPT", "IDX",
    "TXN", "VAC", "STO", "WAL", "REPL", "BACKUP", "PROTO", "CLIENT", "SEC",
    "MON", "OPS", "EXT", "FDW", "ENG", "DIV",
]
FULLWIDTH_COLON = "\uff1a"  # ：
IDEOGRAPHIC_DOT = "\u3002"    # 。

# P0 rows embed the whole title inside the bold marker: **P0-01：title。**
PAT_P0 = re.compile(r"\*\*(P0-\d+)" + re.escape(FULLWIDTH_COLON) + r"([^*]*?)\*\*")
# Capability rows are "- [ ] **FAM-NN** title...。"
_cap_alt = "|".join(f for f in FAMILIES if f != "P0")
PAT_CAP = re.compile(
    r"\*\*((?:" + _cap_alt + r")-\d+)\*\*\s*([^\n]*?)(?:" + re.escape(IDEOGRAPHIC_DOT) + r"|$)"
)


def main() -> int:
    text = open(SRC, encoding="utf-8").read()
    seen = {}
    for gid, title in PAT_P0.findall(text):
        seen.setdefault(gid, title.strip())
    for gid, title in PAT_CAP.findall(text):
        seen.setdefault(gid, title.strip())
    if len(seen) != 273:
        raise SystemExit(
            "expected 273 gap ids, extracted %d; audit format changed?" % len(seen)
        )

    def key(g):
        fam, num = g.split("-")
        return (FAMILIES.index(fam), int(num))

    lines = [
        "# DBMS_C_plus_plus PostgreSQL 18.6 compatibility manifest",
        "# Auto-generated from docs/postgresql-18-gap-audit.md; regenerate with",
        "#   python3 scripts/gen_compat_manifest.py",
        "# status: open | complete  (complete requires full blueprint acceptance evidence)",
        "",
    ]
    for gid in sorted(seen, key=key):
        title = seen[gid].rstrip(IDEOGRAPHIC_DOT).rstrip(".")
        lines.append("- id: %s" % gid)
        lines.append("  title: %s" % title)
        lines.append("  status: open")
        lines.append("")
    os.makedirs(os.path.dirname(DST), exist_ok=True)
    with open(DST, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    print("wrote %s with %d gaps" % (DST, len(seen)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
