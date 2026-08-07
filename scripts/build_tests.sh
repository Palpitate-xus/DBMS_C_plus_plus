#!/bin/bash
# 编译并运行 tests/ 目录下的独立测试程序

set -e

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${SRC_DIR}/build"

# 检测 OpenSSL（与主构建保持一致）
HAS_OPENSSL=0
if pkg-config --exists openssl 2>/dev/null; then
    HAS_OPENSSL=1
    echo "[test-build] OpenSSL detected, TLS support enabled"
else
    echo "[test-build] OpenSSL not found, using TLS stub (plain TCP)"
fi

# Shared source manifest; tests link every production unit except main.cpp.
SOURCES=()
while IFS= read -r src; do
    [[ -z "$src" || "$src" == \#* || "$src" == "src/main.cpp" ]] && continue
    SOURCES+=("$src")
done < <(sed '/^[[:space:]]*#/d;/^[[:space:]]*$/d' "${SRC_DIR}/cmake/dbms_sources.txt")

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
    -Isrc/expression
    -Isrc/replication
    -Isrc/process
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
    has_local_stubs=0
    if rg -q '(^|[[:space:]])(bool checkAdmin|bool checkDB|std::string resolveTableName|bool execute\(|void logSlowQuery|void recordSqlStat)' "$test_file"; then
        has_local_stubs=1
    fi

    # 如果测试文件顶部有 # test_sources: src/... 注释，则使用自定义源文件列表
    if sed -n '1,5p' "$test_file" | grep -qE '^// test_sources:'; then
        custom_sources=$(sed -n '1,5p' "$test_file" | grep -E '^// test_sources:' | cut -d':' -f2-)
        test_sources=()
        for src in $custom_sources; do
            test_sources+=("$src")
        done
    elif [ "$has_local_stubs" -eq 1 ]; then
        test_sources=("${SOURCES[@]}")
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
