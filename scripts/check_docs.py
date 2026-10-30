#!/usr/bin/env python3
"""Check repository Markdown inventory, duplicate files and local file links.

External URLs, fragments and the technical claims in historical records are
outside this check. Fenced code and inline code are excluded from link parsing.
"""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
START = "<!-- DOC_INVENTORY_START -->"
END = "<!-- DOC_INVENTORY_END -->"


def markdown_files(root):
    names = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root
    ).decode().split("\0")
    return sorted({name for name in names if name.lower().endswith(".md") and (root / name).is_file()})


def category(name):
    if name.startswith(("docs/issue-", "docs/issues/")):
        return "历史问题与验证证据"
    if name.startswith("docs/archive/") or name == "RELEASE-NOTES.md":
        return "历史档案/发布快照"
    if name == "CHANGELOG.md":
        return "版本变更"
    if name in {"README.md", "docs/README.md", "docs/MANUAL.md", "docs/PACKAGING.md", "docs/commandsList.md"} or name.startswith("src/"):
        return "使用/模块说明"
    if name in {"docs/all-gaps-todo.md", "docs/implementation-plan.md", "docs/feature-gaps.md", "docs/full-gap-execution-plan.md", "docs/code-review-progress.md"}:
        return "历史计划与进度"
    if name in {"docs/compatibility-contract.md", "docs/postgresql-18-gap-audit.md", "docs/postgresql-comparison.md", "docs/deferred-scope.md"}:
        return "兼容性范围/状态"
    if name in {"docs/postgresql-18-implementation-blueprint.md", "docs/review-closeout-plan.md"}:
        return "目标与验收计划"
    if name in {"docs/performance-report-2026-10-30.md", "docs/test-report.md", "docs/production-status.md"}:
        return "当前验证/状态"
    return "文档与审查记录"


def prose(text):
    result, fence, fence_length = [], None, 0
    for line in text.splitlines():
        match = re.match(r"^\s{0,3}(`{3,}|~{3,})", line)
        if match:
            marker = match.group(1)
            if fence is None:
                fence, fence_length = marker[0], len(marker)
            elif marker[0] == fence and len(marker) >= fence_length and not line[match.end():].strip():
                fence = None
            result.append("")
        else:
            result.append("" if fence else re.sub(r"(`+).*?\1", "", line))
    return "\n".join(result)


def check_links(root, files):
    missing, checked = [], 0
    for name in files:
        text = prose((root / name).read_text())
        patterns = [r"!?\[[^\]\n]*\]\((<[^>]+>|[^\s)]+)(?:\s+[^)]*)?\)",
                    r"^\s{0,3}\[[^\]]+\]:\s*(<[^>]+>|\S+)"]
        for pattern in patterns:
            for match in re.finditer(pattern, text, re.M):
                target = match.group(1).strip("<>")
                if target.startswith(("#", "//")) or re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", target):
                    continue
                path = re.sub(r":\d+(?::\d+)?$", "", unquote(urlsplit(target).path))
                if not path:
                    continue
                checked += 1
                destination = (root / path.lstrip("/")) if path.startswith("/") and not path.startswith("/home/") else root / Path(name).parent / path
                if not destination.exists():
                    missing.append({"file": name, "line": text[:match.start()].count("\n") + 1, "target": target})
    return checked, missing


def update_inventory(root, files):
    path = root / "docs/documentation-inventory.md"
    text = path.read_text()
    if text.count(START) != 1 or text.count(END) != 1:
        raise ValueError("inventory must contain one generated-table marker pair")
    _, missing = check_links(root, files)
    failed = {item["file"] for item in missing}
    evidence = sum(category(name) == "历史问题与验证证据" for name in files)
    rows = [f"本次清单共 **{len(files)}** 份 Markdown，其中 **{evidence}** 份问题与验证证据。",
            "", "| 文档 | 分类 | 本地文件链接 |", "| --- | --- | --- |"]
    for name in files:
        target = Path(os.path.relpath(root / name, path.parent)).as_posix()
        rows.append(f"| [{name}]({target}) | {category(name)} | {'需修正' if name in failed else '通过'} |")
    before, rest = text.split(START)
    _, after = rest.split(END)
    path.write_text(before + START + "\n\n" + "\n".join(rows) + "\n\n" + END + after)


def audit(root):
    files = markdown_files(root)
    groups, empty = defaultdict(list), []
    for name in files:
        data = (root / name).read_bytes()
        groups[hashlib.sha256(data).hexdigest()].append(name)
        if not data.decode().strip():
            empty.append(name)
    checked, missing = check_links(root, files)
    text = (root / "docs/documentation-inventory.md").read_text()
    table = text.split(START)[1].split(END)[0] if START in text and END in text else text
    rows = re.findall(r"^\|\s*\[([^\]]+\.md)\]\([^)]+\)\s*\|", table, re.M)
    return {"markdown_count": len(files), "categories": dict(Counter(map(category, files))),
            "local_file_links_checked": checked, "missing_local_file_links": missing,
            "empty_documents": empty, "identical_document_groups": [g for g in groups.values() if len(g) > 1],
            "inventory_missing": sorted(set(files) - set(rows)),
            "inventory_stale": sorted(set(rows) - set(files)),
            "inventory_duplicate_rows": sorted(name for name, count in Counter(rows).items() if count > 1),
            "scope": "Inline/reference local file targets outside code; external URLs, fragments and technical claim validation excluded"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--update-inventory", action="store_true")
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()
    if args.update_inventory:
        update_inventory(ROOT, markdown_files(ROOT))
    result = audit(ROOT)
    if args.json:
        args.json.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(f"Markdown: {result['markdown_count']}; local file links: {result['local_file_links_checked']}")
    failures = ["missing_local_file_links", "empty_documents", "identical_document_groups",
                "inventory_missing", "inventory_stale", "inventory_duplicate_rows"]
    for name in failures:
        if result[name]:
            print(name + ": " + json.dumps(result[name], ensure_ascii=False))
    success = not any(result[name] for name in failures)
    print("DOCS_CHECK " + ("PASS" if success else "FAIL"))
    raise SystemExit(0 if success else 1)


if __name__ == "__main__":
    main()
