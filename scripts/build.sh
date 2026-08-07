#!/bin/bash
# DBMS 自动构建脚本
# 检测 OpenSSL 并自动选择 TLS 实现

set -e

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${SRC_DIR}/build"

# 检测 OpenSSL
HAS_OPENSSL=0
if pkg-config --exists openssl 2>/dev/null; then
    HAS_OPENSSL=1
    echo "[build] OpenSSL detected, TLS support enabled"
else
    echo "[build] OpenSSL not found, using TLS stub (plain TCP)"
fi

# Source of truth shared with CMake and the test runners.
mapfile -t SOURCES < <(sed '/^[[:space:]]*#/d;/^[[:space:]]*$/d' "${SRC_DIR}/cmake/dbms_sources.txt")

# 选择 TLS 实现
if [ "$HAS_OPENSSL" -eq 1 ]; then
    SOURCES+=(src/network/TLSWrapper.cpp)
    TLS_DEFS="-DHAS_OPENSSL=1"
    TLS_LIBS="-lssl -lcrypto"
else
    SOURCES+=(src/network/TLSWrapper_stub.cpp)
    TLS_DEFS=""
    TLS_LIBS=""
fi

# Include 路径
INCLUDES=(
    -Isrc
    -Isrc/common
    -Isrc/storage
    -Isrc/access
    -Isrc/transaction
    -Isrc/network
    -Isrc/utils
    -Isrc/executor
    -Isrc/commands
    -Isrc/interfaces
    -Isrc/parser
    -Isrc/catalog
    -Isrc/expression
)

# 编译选项
CXXFLAGS="-std=c++17 -O2 -pthread -Wall -Wextra ${TLS_DEFS}"
LDFLAGS="-pthread ${TLS_LIBS}"

echo "[build] Compiling..."
cd "${SRC_DIR}"
g++ ${CXXFLAGS} "${INCLUDES[@]}" "${SOURCES[@]}" -o dbms_main ${LDFLAGS}

echo "[build] Success: ./dbms_main"
