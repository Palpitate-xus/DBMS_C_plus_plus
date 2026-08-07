#!/bin/zsh
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

INC=(-Isrc -Isrc/common -Isrc/storage -Isrc/access -Isrc/transaction -Isrc/network \
     -Isrc/utils -Isrc/executor -Isrc/commands -Isrc/interfaces -Isrc/parser \
     -Isrc/catalog -Isrc/expression)
CXXFLAGS=(-std=c++17 -O2 -pthread -Wall -Wextra)

HAS_OPENSSL=0
if pkg-config --exists openssl 2>/dev/null; then HAS_OPENSSL=1; fi
if [ "$HAS_OPENSSL" -eq 1 ]; then
    TLS_DEFS=(-DHAS_OPENSSL=1)
    TLS_LIBS=(-lssl -lcrypto)
    TLS_SOURCE="src/network/TLSWrapper.cpp"
else
    TLS_DEFS=()
    TLS_LIBS=()
    TLS_SOURCE="src/network/TLSWrapper_stub.cpp"
fi

mkdir -p build/obj

test_name="$1"; shift
# Explicit sources are always rebuilt first for the documented incremental
# workflow. The manifest/dependency pass below also catches headers changed by
# a source not listed on the command line.
for src in "$@"; do
    base=$(basename "$src" .cpp)
    echo "[inc] compiling $src -> build/obj/$base.o"
    g++ "${CXXFLAGS[@]}" "${TLS_DEFS[@]}" "${INC[@]}" -c "$src" -o "build/obj/$base.o"
done

PROJECT_SOURCES=()
while IFS= read -r src; do
    [[ -z "$src" || "$src" == \#* || "$src" == "src/main.cpp" ]] && continue
    PROJECT_SOURCES+=("$src")
done < <(sed '/^[[:space:]]*#/d;/^[[:space:]]*$/d' cmake/dbms_sources.txt)
PROJECT_SOURCES+=("$TLS_SOURCE")
NEWEST_HEADER=$(print -rl src/**/*.(h|hpp)(N.om[1]) 2>/dev/null || true)
for src in "${PROJECT_SOURCES[@]}"; do
    base=$(basename "$src" .cpp)
    obj="build/obj/$base.o"
    if [ ! -f "$obj" ] || [ "$src" -nt "$obj" ] ||
       { [ -n "$NEWEST_HEADER" ] && [ "$NEWEST_HEADER" -nt "$obj" ]; }; then
        echo "[inc] compiling stale project source $src -> $obj"
        g++ "${CXXFLAGS[@]}" "${TLS_DEFS[@]}" "${INC[@]}" -c "$src" -o "$obj"
    fi
done

if [ ! -f build/obj/test_stubs.o ] || [ tests/test_stubs.cpp -nt build/obj/test_stubs.o ] ||
   { [ -n "$NEWEST_HEADER" ] && [ "$NEWEST_HEADER" -nt build/obj/test_stubs.o ]; }; then
    g++ "${CXXFLAGS[@]}" "${TLS_DEFS[@]}" "${INC[@]}" -c tests/test_stubs.cpp -o build/obj/test_stubs.o
fi

# 编译测试本身
echo "[inc] compiling tests/$test_name.cpp"
g++ "${CXXFLAGS[@]}" "${TLS_DEFS[@]}" "${INC[@]}" -c "tests/$test_name.cpp" -o "build/obj/$test_name.o"

# 链接：所有非 *_test.o 的对象 + 本测试对象（排除 main.o，测试自带 main 与桩）
link=()
for o in build/obj/*.o; do
    case "$o" in
        *_test.o) ;;
        */main.o) ;;
        *) link+=("$o");;
    esac
done
if rg -q '(^|[[:space:]])(bool checkAdmin|bool checkDB|std::string resolveTableName|bool execute\(|void logSlowQuery|void recordSqlStat)' "tests/$test_name.cpp"; then
    filtered_link=()
    for o in "${link[@]}"; do
        [[ "$o" == */test_stubs.o ]] || filtered_link+=("$o")
    done
    link=("${filtered_link[@]}")
fi
echo "[inc] linking build/$test_name"
g++ "${CXXFLAGS[@]}" "${link[@]}" "build/obj/$test_name.o" "${TLS_LIBS[@]}" -o "build/$test_name" -pthread

echo "[inc] running build/$test_name"
"build/$test_name"
