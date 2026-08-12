#!/bin/bash
# DBMS 自动构建脚本
# 检测 OpenSSL/TLS 实现并启用必需的 zlib TOAST 压缩

set -euo pipefail

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${SRC_DIR}/build"
. "${SRC_DIR}/scripts/build_common.sh"
if ! dbms_init_build_config "${SRC_DIR}"; then
    exit 1
fi
dbms_main_sources

dbms_print_tls_status build
cd "${SRC_DIR}"
dbms_build_main
