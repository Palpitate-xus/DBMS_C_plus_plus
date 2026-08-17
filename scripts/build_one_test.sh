#!/usr/bin/env bash
# 增量编译并运行单个测试：复用 build/obj/ 下已编译的对象文件。
# 用法:
#   scripts/build_one_test.sh <test_name> [changed_src1.cpp changed_src2.cpp ...]
# 例:
#   scripts/build_one_test.sh policy_test src/commands/DdlExecutor.cpp
# 会先重新编译列出的源文件到 build/obj/，再编译 tests/<test_name>.cpp 并链接所有
# 非 *_test.o 的对象文件运行。

set -e
SRC_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$SRC_DIR"
. "${SRC_DIR}/scripts/build_common.sh"
if ! dbms_init_build_config "${SRC_DIR}"; then
    exit 1
fi
dbms_test_project_sources
dbms_print_tls_status inc

mkdir -p build/obj
CACHE_INVALID=0
if dbms_test_cache_needs_rebuild build/obj; then
    CACHE_INVALID=1
    echo "[inc] build configuration changed; invalidating cached objects"
fi

test_name="$1"; shift
# Explicit sources are always rebuilt first for the documented incremental
# workflow. The manifest/dependency pass below also catches headers changed by
# a source not listed on the command line.
for src in "$@"; do
    base=$(basename "$src" .cpp)
    echo "[inc] compiling $src -> build/obj/$base.o"
    g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_TEST_INCLUDES[@]}" -c "$src" -o "build/obj/$base.o"
done

NEWEST_HEADER="$(dbms_newest_header)"
for src in "${DBMS_PROJECT_SOURCES[@]}"; do
    base=$(basename "$src" .cpp)
    obj="build/obj/$base.o"
    if [ "$CACHE_INVALID" -eq 1 ] || [ ! -f "$obj" ] || [ "$src" -nt "$obj" ] ||
       { [ -n "$NEWEST_HEADER" ] && [ "$NEWEST_HEADER" -nt "$obj" ]; }; then
        echo "[inc] compiling stale project source $src -> $obj"
        g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_PRODUCTION_INCLUDES[@]}" -c "$src" -o "$obj"
    fi
done

if [ "$CACHE_INVALID" -eq 1 ] || [ ! -f build/obj/test_stubs.o ] || [ tests/test_stubs.cpp -nt build/obj/test_stubs.o ] ||
   { [ -n "$NEWEST_HEADER" ] && [ "$NEWEST_HEADER" -nt build/obj/test_stubs.o ]; }; then
    g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_TEST_INCLUDES[@]}" -c tests/test_stubs.cpp -o build/obj/test_stubs.o
fi

# 编译测试本身
echo "[inc] compiling tests/$test_name.cpp"
g++ "${DBMS_CXXFLAGS[@]}" "${DBMS_TEST_INCLUDES[@]}" -c "tests/$test_name.cpp" -o "build/obj/$test_name.o"

# 链接：所有非 *_test.o 的对象 + 本测试对象（排除 main.o，测试自带 main 与桩）
link=()
for o in build/obj/*.o; do
    case "$o" in
        *_test.o) ;;
        */main.o) ;;
        *) link+=("$o");;
    esac
done
if grep -Eq '(^|[[:space:]])(bool checkAdmin|bool checkDB|std::string resolveTableName|bool execute\(|void logSlowQuery|void recordSqlStat)' "tests/$test_name.cpp"; then
    filtered_link=()
    for o in "${link[@]}"; do
        [[ "$o" == */test_stubs.o ]] || filtered_link+=("$o")
    done
    link=("${filtered_link[@]}")
fi
echo "[inc] linking build/$test_name"
g++ "${DBMS_CXXFLAGS[@]}" "${link[@]}" "build/obj/$test_name.o" "${DBMS_LDFLAGS[@]}" -o "build/$test_name"

echo "[inc] running build/$test_name"
dbms_run_isolated_test "$test_name" "${SRC_DIR}/build/$test_name"
dbms_write_test_cache_signature build/obj
