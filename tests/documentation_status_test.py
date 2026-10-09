#!/usr/bin/env python3
"""Prevent release/status documentation from becoming authoritative twice."""

from pathlib import Path
import re


REPO = Path(__file__).resolve().parents[1]


def read(path):
    return (REPO / path).read_text(encoding="utf-8")


def main():
    header = read("src/common/version.h")
    version = re.search(
        r'^#define DBMS_VERSION_STRING "([^"]+)"$', header, re.MULTILINE).group(1)
    readme = read("README.md")
    release_notes = read("RELEASE-NOTES.md")
    changelog = read("CHANGELOG.md")
    feature_gaps = read("docs/feature-gaps.md")
    production = read("docs/production-status.md")

    canonical = ("postgresql-18-gap-audit.md", "gap-progress.json",
                 "scripts/check_gap_progress.py")
    for name, document in (("feature-gaps", feature_gaps),
                           ("production-status", production)):
        for marker in canonical:
            assert marker in document, "%s omits %s" % (name, marker)

    # README is an evergreen project entry point, not another release/status
    # snapshot. Actual build/header/changelog/package version checks remain in
    # version_consistency_test.py.
    for section in ("## 构建", "## 运行", "## 测试", "## 文档",
                    "## 参与贡献", "## 许可证"):
        assert section in readme, "README omits %s" % section
    for stale_marker in (*canonical, "发行标识为 v", "当前状态（", "性能与并发硬化轮次",
                         "### 并发测试结果", "### 新增功能 (Phase"):
        assert stale_marker not in readme, "README retains %s" % stale_marker
    assert "历史发布快照" in release_notes
    assert "不是当前工作树" in release_notes
    assert "156 个 `*_test.cpp` 测试源" in release_notes
    assert changelog.count("156 个 `*_test.cpp` 测试源") == 2
    assert "历史专题清单" in feature_gaps
    assert "下文所有 PASS 数仅表示" in feature_gaps
    assert "历史证据" in production
    assert "ci.yml.disabled" in production
    assert ("compare/v%s...HEAD" % version) in changelog
    assert ("[%s]:" % version) in changelog

    active_workflows = [path for path in (REPO / ".github/workflows").glob("*")
                        if path.suffix in (".yml", ".yaml")]
    assert active_workflows == [], active_workflows
    print("[DOCUMENTATION STATUS] canonical live ledger; historical baselines labelled")


if __name__ == "__main__":
    main()
