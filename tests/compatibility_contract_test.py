#!/usr/bin/env python3
"""Keep compatibility claims separate from product and protocol versions."""

from pathlib import Path
import re


REPO = Path(__file__).resolve().parents[1]


def read(path):
    return (REPO / path).read_text(encoding="utf-8")


def main():
    contract = read("docs/compatibility-contract.md")
    feature_gate = read("src/common/FeatureGate.h")
    protocol = read("src/network/PostgresProtocol.cpp")
    version = read("src/common/version.h")
    readme = read("README.md")
    production = read("docs/production-status.md")

    product_version = re.search(
        r'^#define DBMS_VERSION_STRING "([^"]+)"$', version,
        re.MULTILINE).group(1)
    modes = dict(re.findall(
        r'kCompatMode(\w+)\s*=\s*"([^"]+)"', feature_gate))

    assert product_version == "0.2.0"
    assert modes == {"Postgresql18": "postgresql18", "Extended": "extended"}
    assert "constexpr uint32_t kProtocol30 = 0x00030000" in protocol
    assert "产品版本 | `0.2.0`" in contract
    assert "差分基线 `18.6`" in contract
    assert "wire protocol 上限 | `3.0` (`196608`)" in contract
    assert "`postgresql18` 是默认模式" in contract
    assert "A 门和 B 门均未宣告通过" in contract
    assert "deferred_by_user" in contract
    assert "scripts/check_gap_progress.py --require-complete" in contract
    for document in (readme, production):
        assert "compatibility-contract.md" in document
        assert "wire protocol 3.0" in document
    print("[COMPATIBILITY CONTRACT] product/behavior/wire/extended claims separated")


if __name__ == "__main__":
    main()
