#!/bin/bash
# DBMS 自动构建脚本
# 检测 OpenSSL 并自动选择 TLS 实现

set -euo pipefail

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${SRC_DIR}/build"
. "${SRC_DIR}/scripts/build_common.sh"
dbms_init_build_config "${SRC_DIR}"
dbms_main_sources

dbms_print_tls_status build
echo "[build] Compiling..."
cd "${SRC_DIR}"
g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_PRODUCTION_INCLUDES[@]}" \
    "${DBMS_MAIN_SOURCES[@]}" -o dbms_main "${DBMS_LDFLAGS[@]}"

echo "[build] Success: ./dbms_main"
