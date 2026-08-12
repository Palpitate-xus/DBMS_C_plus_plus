#!/bin/bash
# Compile and run every standalone test under tests/.
# Production units are compiled once and each test is still linked and run
# independently. This keeps the script suitable for CI instead of recompiling
# the complete DBMS for every test case.

set -u

SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="${SRC_DIR}/build/test_obj"
. "${SRC_DIR}/scripts/build_common.sh"
if ! dbms_init_build_config "${SRC_DIR}"; then
    exit 1
fi
dbms_test_project_sources

dbms_print_tls_status test-build
dbms_main_sources

cd "${SRC_DIR}"
mkdir -p "${BUILD_DIR}"
CACHE_INVALID=0
if dbms_test_cache_needs_rebuild "${BUILD_DIR}"; then
    CACHE_INVALID=1
    echo "[test-build] Build configuration changed; invalidating cached objects"
fi

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
    if [ "$CACHE_INVALID" -eq 1 ] || [ ! -f "$object" ] || [ "$source" -nt "$object" ] || \
       find src -type f \( -name '*.h' -o -name '*.hpp' \) -newer "$object" -print -quit | grep -q .; then
        echo "[test-build] Compiling production unit ${source} ..." >&2
        if ! g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_PRODUCTION_INCLUDES[@]}" -c "$source" -o "$object"; then
            echo "[test-build] COMPILE FAILED: ${source}" >&2
            return 1
        fi
    fi
    echo "$object"
}

PROJECT_OBJECTS=()
FAILED=0
if ! dbms_build_main; then
    echo "[test-build] Production binary is unavailable; refusing to run tests" >&2
    exit 1
fi

for source in "${DBMS_PROJECT_SOURCES[@]}"; do
    if object="$(compile_source "$source")"; then
        PROJECT_OBJECTS+=("$object")
    else
        FAILED=1
    fi
done

TEST_STUB_OBJECT="$(object_for_source tests/test_stubs.cpp)"
if [ "$CACHE_INVALID" -eq 1 ] || [ ! -f "$TEST_STUB_OBJECT" ] || [ tests/test_stubs.cpp -nt "$TEST_STUB_OBJECT" ]; then
    echo "[test-build] Compiling shared test stubs ..."
    if ! g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_TEST_INCLUDES[@]}" -c tests/test_stubs.cpp -o "$TEST_STUB_OBJECT"; then
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
    if ! g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_TEST_INCLUDES[@]}" -c "$test_file" -o "$test_object"; then
        echo "[test-build] ${name} COMPILE FAILED" >&2
        FAILED=1
        continue
    fi
    echo "[test-build] Linking/running ${binary} ..."
    if ! g++ "${DBMS_CXXFLAGS[@]}" "$test_object" "${link_objects[@]}" "${DBMS_LDFLAGS[@]}" -o "$binary"; then
        echo "[test-build] ${name} LINK FAILED" >&2
        FAILED=1
        continue
    fi
    if dbms_run_isolated_test "$name" "$binary"; then
        echo "[test-build] ${name} PASSED"
    else
        echo "[test-build] ${name} FAILED"
        FAILED=1
    fi
    echo
done

for e2e_test in "${DBMS_E2E_TESTS[@]}"; do
    echo "[test-build] Running ${e2e_test} ..."
    if python3 "${e2e_test}"; then
        echo "[test-build] ${e2e_test} PASSED"
    else
        echo "[test-build] ${e2e_test} FAILED"
        FAILED=1
    fi
done

if [ "$FAILED" -ne 0 ]; then
    echo "[test-build] Some tests failed" >&2
    exit 1
fi

dbms_write_test_cache_signature "${BUILD_DIR}"
echo "[test-build] All tests passed"
