#!/bin/bash
# 编译并运行 tests/ 目录下的独立测试程序

set -e

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${SRC_DIR}/build"

# 检测 OpenSSL（与主构建保持一致）
HAS_OPENSSL=0
if pkg-config --exists openssl 2>/dev/null || [ -f /usr/include/openssl/ssl.h ]; then
    HAS_OPENSSL=1
    echo "[test-build] OpenSSL detected, TLS support enabled"
else
    echo "[test-build] OpenSSL not found, using TLS stub (plain TCP)"
fi

# 共享源文件列表（与 build.sh 一致，但不含 src/main.cpp）
SOURCES=(
    src/commands/TableManage.cpp
    src/executor/ExecutionPlan.cpp
    src/storage/BufferPool.cpp
    src/storage/PageAllocator.cpp
    src/storage/Page.cpp
    src/storage/PgPage.cpp
    src/storage/PageWrapper.cpp
    src/storage/FreeSpaceMap.cpp
    src/storage/VisibilityMap.cpp
    src/storage/CommitLog.cpp
    src/storage/WAL.cpp
    src/access/BPTree.cpp
    src/access/HashIndex.cpp
    src/access/SPGiSTIndex.cpp
    src/access/BPTreeIndexAM.cpp
    src/access/HashIndexAM.cpp
    src/transaction/LockManager.cpp
    src/transaction/TxnIdGenerator.cpp
    src/network/NetworkServer.cpp
    src/common/Config.cpp
    src/common/GUC.cpp
    src/parser/parser.cpp
    src/catalog/catalog.cpp
    src/catalog/oid.cpp
    src/catalog/systables.cpp
    src/catalog/migrate.cpp
)

if [ "$HAS_OPENSSL" -eq 1 ]; then
    SOURCES+=(src/network/TLSWrapper.cpp)
    TLS_DEFS="-DHAS_OPENSSL=1"
    TLS_LIBS="-lssl -lcrypto"
else
    SOURCES+=(src/network/TLSWrapper_stub.cpp)
    TLS_DEFS=""
    TLS_LIBS=""
fi

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
)

CXXFLAGS="-std=c++17 -O2 -pthread -Wall -Wextra ${TLS_DEFS}"
LDFLAGS="-pthread ${TLS_LIBS}"

mkdir -p "${BUILD_DIR}"
cd "${SRC_DIR}"

FAILED=0

for test_file in tests/*_test.cpp; do
    [ -e "$test_file" ] || continue
    name=$(basename "$test_file" .cpp)
    out="${BUILD_DIR}/${name}"

    # 如果测试文件顶部有 # test_sources: src/... 注释，则使用自定义源文件列表
    if sed -n '1,5p' "$test_file" | grep -qE '^// test_sources:'; then
        custom_sources=$(sed -n '1,5p' "$test_file" | grep -E '^// test_sources:' | cut -d':' -f2-)
        test_sources=()
        for src in $custom_sources; do
            test_sources+=("$src")
        done
    else
        test_sources=("${SOURCES[@]}" tests/test_stubs.cpp)
    fi

    echo "[test-build] Compiling ${test_file} ..."
    if g++ ${CXXFLAGS} "${INCLUDES[@]}" "${test_sources[@]}" "$test_file" -o "$out" ${LDFLAGS}; then
        echo "[test-build] Running ${out} ..."
        if "$out"; then
            echo "[test-build] ${name} PASSED"
        else
            echo "[test-build] ${name} FAILED"
            FAILED=1
        fi
    else
        echo "[test-build] ${name} COMPILE FAILED"
        FAILED=1
    fi
    echo ""
done

if [ "$FAILED" -ne 0 ]; then
    echo "[test-build] Some tests failed"
    exit 1
fi

echo "[test-build] All tests passed"
