#!/bin/bash
# Compile and run every standalone test under tests/.
# Production units are compiled once and each test is still linked and run
# independently. This keeps the script suitable for CI instead of recompiling
# the complete DBMS for every test case.

set -u

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${SRC_DIR}/build/test_obj"

HAS_OPENSSL=0
if pkg-config --exists openssl 2>/dev/null; then
    HAS_OPENSSL=1
    echo "[test-build] OpenSSL detected, TLS support enabled"
else
    echo "[test-build] OpenSSL not found, using TLS stub (plain TCP)"
fi

INCLUDES=(
    -Isrc -Isrc/common -Isrc/storage -Isrc/access -Isrc/transaction
    -Isrc/network -Isrc/utils -Isrc/executor -Isrc/commands -Isrc/interfaces
    -Isrc/parser -Isrc/catalog -Isrc/expression -Isrc/replication -Isrc/process
)
CXXFLAGS=(-std=c++17 -O2 -pthread -Wall -Wextra)
LDFLAGS=(-pthread)
if [ "$HAS_OPENSSL" -eq 1 ]; then
    CXXFLAGS+=(-DHAS_OPENSSL=1)
    LDFLAGS+=(-lssl -lcrypto)
    TLS_SOURCE="src/network/TLSWrapper.cpp"
else
    TLS_SOURCE="src/network/TLSWrapper_stub.cpp"
fi

mapfile -t MANIFEST_SOURCES < <(
    sed '/^[[:space:]]*#/d;/^[[:space:]]*$/d' "${SRC_DIR}/cmake/dbms_sources.txt"
)
PROJECT_SOURCES=()
for src in "${MANIFEST_SOURCES[@]}"; do
    [ "$src" = "src/main.cpp" ] || PROJECT_SOURCES+=("$src")
done
PROJECT_SOURCES+=("$TLS_SOURCE")

cd "${SRC_DIR}"
mkdir -p "${BUILD_DIR}"

object_for_source() {
    local source="$1"
    local base
    base="$(basename "$source" .cpp)"
    echo "${BUILD_DIR}/${base}.o"
}

compile_source() {
    local source="$1"
    local object
    object="$(object_for_source "$source")"
    if [ ! -f "$object" ] || [ "$source" -nt "$object" ] || \
       find src -type f \( -name '*.h' -o -name '*.hpp' \) -newer "$object" -print -quit | grep -q .; then
        echo "[test-build] Compiling production unit ${source} ..." >&2
        if ! g++ "${CXXFLAGS[@]}" "${INCLUDES[@]}" -c "$source" -o "$object"; then
            echo "[test-build] COMPILE FAILED: ${source}" >&2
            return 1
        fi
    fi
    echo "$object"
}

PROJECT_OBJECTS=()
FAILED=0
for source in "${PROJECT_SOURCES[@]}"; do
    if object="$(compile_source "$source")"; then
        PROJECT_OBJECTS+=("$object")
    else
        FAILED=1
    fi
done

TEST_STUB_OBJECT="$(object_for_source tests/test_stubs.cpp)"
if [ ! -f "$TEST_STUB_OBJECT" ] || [ tests/test_stubs.cpp -nt "$TEST_STUB_OBJECT" ]; then
    echo "[test-build] Compiling shared test stubs ..."
    if ! g++ "${CXXFLAGS[@]}" "${INCLUDES[@]}" -c tests/test_stubs.cpp -o "$TEST_STUB_OBJECT"; then
        echo "[test-build] COMPILE FAILED: tests/test_stubs.cpp" >&2
        FAILED=1
    fi
fi

for test_file in tests/*_test.cpp; do
    [ -e "$test_file" ] || continue
    name="$(basename "$test_file" .cpp)"
    test_object="${BUILD_DIR}/${name}.o"
    binary="${SRC_DIR}/build/${name}"
    has_local_stubs=0
    if rg -q '(^|[[:space:]])(bool checkAdmin|bool checkDB|std::string resolveTableName|bool execute\(|void logSlowQuery|void recordSqlStat)' "$test_file"; then
        has_local_stubs=1
    fi

    link_objects=()
    custom_header="$(sed -n '1,5p' "$test_file" | grep -E '^// test_sources:' || true)"
    if [ -n "$custom_header" ]; then
        # The annotation documents the test's narrow dependency intent, but
        # the complete production object set is required for transitive
        # runtime symbols (for example sequence functions in ExprEvaluator).
        # Objects are already compiled once above, so this remains cheap.
        link_objects=("${PROJECT_OBJECTS[@]}" "$TEST_STUB_OBJECT")
    else
        link_objects=("${PROJECT_OBJECTS[@]}")
        if [ "$has_local_stubs" -eq 0 ]; then
            link_objects+=("$TEST_STUB_OBJECT")
        fi
    fi

    echo "[test-build] Compiling ${test_file} ..."
    if ! g++ "${CXXFLAGS[@]}" "${INCLUDES[@]}" -c "$test_file" -o "$test_object"; then
        echo "[test-build] ${name} COMPILE FAILED" >&2
        FAILED=1
        continue
    fi
    echo "[test-build] Linking/running ${binary} ..."
    if ! g++ "${CXXFLAGS[@]}" "$test_object" "${link_objects[@]}" "${LDFLAGS[@]}" -o "$binary"; then
        echo "[test-build] ${name} LINK FAILED" >&2
        FAILED=1
        continue
    fi
    if "$binary"; then
        echo "[test-build] ${name} PASSED"
    else
        echo "[test-build] ${name} FAILED"
        FAILED=1
    fi
    echo
done

echo "[test-build] Running tests/postgres_protocol_test.py ..."
if python3 tests/postgres_protocol_test.py; then
    echo "[test-build] postgres_protocol_test PASSED"
else
    echo "[test-build] postgres_protocol_test FAILED"
    FAILED=1
fi

if [ "$FAILED" -ne 0 ]; then
    echo "[test-build] Some tests failed" >&2
    exit 1
fi

echo "[test-build] All tests passed"
