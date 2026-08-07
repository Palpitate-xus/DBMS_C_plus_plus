#!/usr/bin/env bash
# Fast batch test runner: build project objects if stale/missing, compile+link+run each test,
# report PASS/FAIL by exit code.
SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SRC_DIR"
. "${SRC_DIR}/scripts/build_common.sh"
dbms_init_build_config "${SRC_DIR}"
dbms_test_project_sources
dbms_print_tls_status fast-test

mkdir -p build/obj
CACHE_INVALID=0
if dbms_cache_needs_rebuild build/obj; then
    CACHE_INVALID=1
    echo "Build configuration changed; invalidating cached objects"
fi

# Conservative header dependency check: if any src header changed, rebuild all project objects.
NEWEST_HEADER="$(dbms_newest_header)"

for src in "${DBMS_PROJECT_SOURCES[@]}"; do
    obj="build/obj/$(basename "$src" .cpp).o"
    if [ "$CACHE_INVALID" -eq 1 ] || [ ! -f "$obj" ] || [ "$src" -nt "$obj" ] || { [ -n "$NEWEST_HEADER" ] && [ "$NEWEST_HEADER" -nt "$obj" ]; }; then
        if ! g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_PRODUCTION_INCLUDES[@]}" -c "$src" -o "$obj" 2>/tmp/project_obj.err; then
            echo "COMPILE-FAIL project object $src"; exit 1
        fi
    fi
done

# Ensure shared test stubs (weak globals referenced by project objects) are available.
if [ "$CACHE_INVALID" -eq 1 ] || [ ! -f build/obj/test_stubs.o ] || [ tests/test_stubs.cpp -nt build/obj/test_stubs.o ] || { [ -n "$NEWEST_HEADER" ] && [ "$NEWEST_HEADER" -nt build/obj/test_stubs.o ]; }; then
    if ! g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_TEST_INCLUDES[@]}" -c tests/test_stubs.cpp -o build/obj/test_stubs.o 2>/tmp/test_stubs.err; then
        echo "COMPILE-FAIL test_stubs.cpp"; exit 1
    fi
fi

link=()
for o in build/obj/*.o; do
    case "$o" in
        *_test.o) ;;
        */main.o) ;;
        *) link+=("$o");;
    esac
done

pass=0; fail=0; failed=()
for tf in tests/*_test.cpp; do
    name=$(basename "$tf" .cpp)
    if ! g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_TEST_INCLUDES[@]}" -c "$tf" -o "build/obj/$name.o" 2>"/tmp/${name}.cerr"; then
        echo "COMPILE-FAIL $name"; fail=$((fail+1)); failed+=("$name(compile)"); continue
    fi
    link_for_test=("${link[@]}")
    if rg -q '(^|[[:space:]])(bool checkAdmin|bool checkDB|std::string resolveTableName|bool execute\(|void logSlowQuery|void recordSqlStat)' "$tf"; then
        link_for_test=()
        for obj in "${link[@]}"; do
            [[ "$obj" == */test_stubs.o ]] || link_for_test+=("$obj")
        done
    fi
    if ! g++ "${DBMS_CXXFLAGS[@]}" "build/obj/$name.o" "${link_for_test[@]}" "${DBMS_LDFLAGS[@]}" -o "build/$name" 2>"/tmp/${name}.lerr"; then
        echo "LINK-FAIL $name"; fail=$((fail+1)); failed+=("$name(link)"); continue
    fi
    if ./build/$name >/dev/null 2>&1; then
        pass=$((pass+1))
    else
        echo "RUN-FAIL $name (exit $?)"; fail=$((fail+1)); failed+=("$name")
    fi
done

for e2e_test in "${DBMS_E2E_TESTS[@]}"; do
    if python3 "${e2e_test}" >/dev/null 2>&1; then
        pass=$((pass+1))
    else
        test_name=$(basename "${e2e_test}" .py)
        echo "RUN-FAIL ${test_name}"
        fail=$((fail+1)); failed+=("${test_name}")
    fi
done
echo "=================================="
echo "PASS=$pass FAIL=$fail"
if [[ $fail -gt 0 ]]; then
    echo "FAILED: ${failed[@]}"
    exit 1
fi
dbms_write_cache_signature build/obj
exit 0
